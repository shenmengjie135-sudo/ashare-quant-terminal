# -*- coding: utf-8 -*-
"""
A股交易系统工具生成器
服务端抓取【沪市主板 + 深市主板】真实K线 -> 计算信号 -> 生成自带数据的HTML
数据源：新浪（股票名单）+ 腾讯（前复权日K）
同时拉取长历史（约800根）做策略回测，输出胜率/盈亏比/最大回撤/年化。
数据取不到时一律留空(None)，前端显示 "—"，绝不填充模拟数据。
"""
import json, subprocess, sys, threading, time
from array import array
from concurrent.futures import ThreadPoolExecutor

HIST = 800          # 每只股票拉取的历史K线根数（约3.2年，用于回测）

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
OUT_HTML = "index.html"

# 沪市主板（排除科创板 688）/ 深市主板（排除创业板 300、301）
SH_MAIN = ("600", "601", "603", "605")
SZ_MAIN = ("000", "001", "002", "003")

# 腾讯前复权日K候选端点（注意：腾讯 WAF 是按「路径」封的，fqkline 被封时 newfqkline 常仍可用）
ALL_KLINE_HOSTS = ("https://ifzq.gtimg.cn/appstock/app/newfqkline/get",
                   "https://web.ifzq.gtimg.cn/appstock/app/newfqkline/get",
                   "https://ifzq.gtimg.cn/appstock/app/fqkline/get",
                   "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get",
                   "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/fqkline/get")
KLINE_HOSTS = ALL_KLINE_HOSTS       # 实际使用的端点（探测后收窄为单个）

# 全局请求限速（避免触发行情站 WAF）
_rl_lock = threading.Lock()
_rl_next = [0.0]
MIN_INTERVAL = 0.05

# WAF 冷却：一旦检测到拦截页，所有线程暂停请求一段时间
_waf_lock = threading.Lock()
_waf_until = [0.0]

# WAF 拦截计数 + 锁定主机被判定拦截后的静默期（快速失败，避免逐只慢重试）
_waf_hits = [0]
_host_block_until = [0.0]


def _throttle():
    with _rl_lock:
        now = time.time()
        if _rl_next[0] > now:
            time.sleep(_rl_next[0] - now)
        _rl_next[0] = max(_rl_next[0], time.time()) + MIN_INTERVAL


def _waf_cooldown(sec):
    with _waf_lock:
        _waf_until[0] = max(_waf_until[0], time.time() + sec)


def _wait_waf():
    while True:
        with _waf_lock:
            d = _waf_until[0] - time.time()
        if d <= 0:
            return
        time.sleep(min(d, 1.0))


def fetch_json(url, referer="https://finance.sina.com.cn", tries=3):
    """用系统 curl.exe 取数（Python OpenSSL 的 TLS 指纹会被行情站 WAF 拦截）。"""
    last = None
    for i in range(tries):
        _wait_waf()
        _throttle()
        try:
            r = subprocess.run(
                ["curl.exe", "-s", "-m", "20", "-H", "User-Agent: " + UA,
                 "-H", "Referer: " + referer, url],
                capture_output=True, timeout=25)
            txt = r.stdout.decode("utf-8", "ignore").strip()
            if txt.startswith("<"):
                # WAF 挑战页：让全局冷却，随后重试本次请求
                _waf_hits[0] += 1
                _waf_cooldown(6.0 + 4.0 * i)
                last = "waf-challenge"
                continue
            if txt:
                return json.loads(txt)
            last = "empty response"
        except Exception as e:
            last = e
        time.sleep(0.3 * (i + 1))
    print("  ! 请求失败:", url.split("?")[0], last, file=sys.stderr)
    return None


def get_node_list(node, maxpage=40):
    """新浪：按板块 node 分页抓全部股票（num=100/页）。"""
    out = []
    for page in range(1, maxpage + 1):
        url = ("https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/"
               "Market_Center.getHQNodeData?page=%d&num=100&sort=symbol&asc=1"
               "&node=%s&symbol=&_s_r_a=page" % (page, node))
        d = fetch_json(url, referer="https://finance.sina.com.cn")
        if not d or not isinstance(d, list) or not d:
            break
        out += d
        if len(d) < 100:
            break
    return out


def get_mainboard_list():
    """沪市主板 + 深市主板名单；排除科创板(688)、创业板(300/301)，排除 ST/退市。"""
    raw = get_node_list("sh_a") + get_node_list("sz_a")
    pool, seen = [], set()
    for it in raw:
        code = str(it.get("code") or "")
        name = it.get("name") or ""
        if not code or len(code) != 6:
            continue
        if not (code.startswith(SH_MAIN) or code.startswith(SZ_MAIN)):
            continue
        if not name or "ST" in name or "退" in name:
            continue
        if code in seen:
            continue
        seen.add(code)
        pool.append(it)
    return pool


def get_kline_tencent(symbol, lmt=70):
    """腾讯：前复权日K。symbol 形如 sh600519 / sz000001 / sh000300。多主机容错。"""
    if time.time() < _host_block_until[0]:
        return None                      # 当前主机已被判定拦截，静默期内快速失败
    hits0 = _waf_hits[0]
    for host in KLINE_HOSTS:
        url = "%s?param=%s,day,,,%d,qfq" % (host, symbol, lmt)
        d = fetch_json(url, referer="https://gu.qq.com/", tries=2)
        if not d or d.get("code") != 0 or not d.get("data"):
            continue
        node = d["data"].get(symbol)
        if not node:
            continue
        arr = node.get("qfqday") or node.get("day")
        if not arr:
            continue
        bars = []
        for row in arr:
            try:
                bars.append({"d": row[0], "o": float(row[1]), "c": float(row[2]),
                             "h": float(row[3]), "l": float(row[4]), "v": float(row[5])})
            except (ValueError, IndexError):
                continue
        if bars:
            return {"bars": bars}
    if _waf_hits[0] > hits0:
        # 本次出现 WAF 拦截：静默一段时间，让本轮尽快结束并触发主机重探
        _host_block_until[0] = time.time() + 20
    return None


def get_kline_sina(symbol, lmt=70):
    """新浪：日K（不复权，即真实成交价）。腾讯前复权接口被 WAF 限流时的备用源。"""
    url = ("https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/"
           "CN_MarketData.getKLineData?symbol=%s&scale=240&ma=no&datalen=%d"
           % (symbol, lmt))
    d = fetch_json(url, referer="https://finance.sina.com.cn/")
    if not isinstance(d, list) or not d:
        return None
    bars = []
    for row in d:
        try:
            bars.append({"d": row["day"], "o": float(row["open"]), "c": float(row["close"]),
                         "h": float(row["high"]), "l": float(row["low"]),
                         "v": float(row["volume"])})
        except (KeyError, ValueError, TypeError):
            continue
    return {"bars": bars} if bars else None


# 运行时探测确定：tencent=前复权 / sina=不复权
KLINE_SOURCE = "tencent"


def get_kline(symbol, lmt=70):
    """按运行时探测确定的数据源取K线（整轮只用一个源，避免复权口径混用）。"""
    if KLINE_SOURCE == "sina":
        return get_kline_sina(symbol, lmt)
    return get_kline_tencent(symbol, lmt)


def probe_kline_host():
    """依次探测腾讯候选主机，返回第一个能取到前复权日K的主机；全部被 WAF 拦截返回 None。"""
    _host_block_until[0] = 0.0           # 重探时解除静默
    for host in ALL_KLINE_HOSTS:
        url = "%s?param=sh600519,day,,,5,qfq" % host
        d = fetch_json(url, referer="https://gu.qq.com/", tries=1)
        if d and d.get("code") == 0 and d.get("data", {}).get("sh600519"):
            return host
    return None


def avg(xs):
    return sum(xs) / len(xs) if xs else None


def analyze(bars, env_ok):
    if not bars or len(bars) < 60:
        return None
    closes = [b["c"] for b in bars]
    last = bars[-1]
    ma20 = avg(closes[-20:])
    ma20_prev = avg(closes[-21:-1])
    ma60 = avg(closes[-60:])
    avg5v = avg([b["v"] for b in bars[-6:-1]])
    vol_ratio = (last["v"] / avg5v) if avg5v else None
    c = {
        "aboveMA20": last["c"] > ma20,
        "ma20Up": ma20 > ma20_prev,
        "ma20gt60": ma20 > ma60,
        "volSurge": (vol_ratio is not None and vol_ratio > 1),
    }
    stock_ok = c["aboveMA20"] and c["ma20Up"] and c["ma20gt60"] and c["volSurge"]
    allbuy = stock_ok and bool(env_ok)
    pct = None
    if len(bars) >= 2:
        pct = (last["c"] / bars[-2]["c"] - 1) * 100
    return {
        "close": round(last["c"], 2),
        "pct": round(pct, 2) if pct is not None else None,
        "ma20": round(ma20, 2),
        "ma60": round(ma60, 2),
        "volRatio": round(vol_ratio, 2) if vol_ratio is not None else None,
        "high20": round(max(b["h"] for b in bars[-20:]), 2),
        "lastDate": last["d"],
        "c": c,
        "stockOk": bool(stock_ok),
        "all": allbuy,
    }


# ============================ 回测引擎 ============================
NAN = float("nan")

BT_CFG = {"capital": 1000000.0, "maxPos": 5, "perPos": 0.20,
          "costBuy": 0.0003, "costSell": 0.0008, "stop": 0.08,
          "take": 0.20, "trail": 0.10,
          "grossUp": 0.70,      # 大盘在20日线上方：总仓位上限（规则：最多7成）
          "grossDown": 0.30}    # 大盘跌破20日线：总仓位上限（规则：降至3成）


def make_arrays(bars, T, cal_pos):
    """把该股K线对齐到全局交易日历（缺失日填 NaN），返回紧凑数值数组。"""
    o = array("d", [NAN]) * T
    c = array("d", [NAN]) * T
    h = array("d", [NAN]) * T
    l = array("d", [NAN]) * T
    v = array("d", [NAN]) * T
    n = 0
    for b in bars:
        p = cal_pos.get(b["d"])
        if p is None:
            continue
        o[p] = b["o"]; c[p] = b["c"]; h[p] = b["h"]; l[p] = b["l"]; v[p] = b["v"]
        n += 1
    return o, c, h, l, v, n


def rolling_ma(arr, w, T):
    """滑动均线：窗口内必须全部有值，否则为 NaN。"""
    out = array("d", [NAN]) * T
    ps = array("d", [0.0]) * (T + 1)
    pc = array("i", [0]) * (T + 1)
    for i in range(T):
        x = arr[i]
        if x != x:
            ps[i + 1] = ps[i]; pc[i + 1] = pc[i]
        else:
            ps[i + 1] = ps[i] + x; pc[i + 1] = pc[i] + 1
    for i in range(w - 1, T):
        if pc[i + 1] - pc[i + 1 - w] == w:
            out[i] = (ps[i + 1] - ps[i + 1 - w]) / w
    return out


def rolling_vr(v, w, T):
    """量比：当日成交量 / 前 w 日均量。"""
    out = array("d", [NAN]) * T
    ps = array("d", [0.0]) * (T + 1)
    pc = array("i", [0]) * (T + 1)
    for i in range(T):
        x = v[i]
        if x != x:
            ps[i + 1] = ps[i]; pc[i + 1] = pc[i]
        else:
            ps[i + 1] = ps[i] + x; pc[i + 1] = pc[i] + 1
    for i in range(w, T):
        cur = v[i]
        if cur != cur:
            continue
        j = i - w
        if pc[i] - pc[j] == w:
            s = ps[i] - ps[j]
            if s > 0:
                out[i] = cur / (s / w)
    return out


def _last_close(st, i):
    c = st["c"]
    while i >= 0:
        x = c[i]
        if x == x:
            return x
        i -= 1
    return NAN


def run_backtest(uni, T, cal, idx_close, idx_above, cfg=None):
    """组合级回测：按日扫描买点 -> 次日开盘买入 -> 逐日按卖出规则离场。"""
    cfg = cfg or BT_CFG
    CAP = cfg["capital"]; MAXP = cfg["maxPos"]; PER = cfg["perPos"]
    CB = cfg["costBuy"]; CS = cfg["costSell"]
    STOP = cfg["stop"]; TAKE = cfg["take"]; TRAIL = cfg["trail"]
    GUP = cfg.get("grossUp", 1.0); GDOWN = cfg.get("grossDown", 1.0)

    cash = CAP
    positions = []
    trades = []
    equity = array("d")
    pending = []
    peak_eq = CAP
    maxdd = 0.0

    def close_full(p, i, px, reason):
        nonlocal cash
        q = p["shares"]
        if q <= 0:
            return
        proceeds = q * px * (1 - CS)
        pnl = proceeds - p["spent"]
        cash += proceeds
        p["realized"] += pnl
        p["shares"] = 0
        p["spent"] = 0.0
        # 整笔平仓：记录累计盈亏（含此前分批止盈部分），保证明细与资金曲线口径一致
        total = p["realized"]
        trades.append({
            "code": p["code"], "name": p["name"],
            "inDate": p["inDate"], "outDate": cal[i],
            "inPx": round(p["entry"], 2), "outPx": round(px, 2),
            "days": i - p["iEntry"],
            "ret": round(total / p["cost0"] * 100, 2) if p["cost0"] else None,
            "pnl": round(total),
            "reason": reason,
        })

    def sell_part(p, i, px, frac):
        nonlocal cash
        q = int(p["shares"] * frac / 100) * 100
        if q <= 0 or q >= p["shares"]:
            q = p["shares"]
        proceeds = q * px * (1 - CS)
        basis = p["spent"] * (q / p["shares"])
        cash += proceeds
        p["realized"] += proceeds - basis
        p["spent"] -= basis
        p["shares"] -= q

    for i in range(T):
        # A) 开盘：执行前一日收盘生成的买点（次日开盘买入）
        if pending:
            eq_prev = equity[-1] if equity else CAP
            # 大盘环境（前一日收盘状态）决定总仓位上限：线上最多 GUP，线下最多 GDOWN
            cap_gross = GUP if (i >= 1 and idx_above[i - 1]) else GDOWN
            mv = 0.0
            for p in positions:
                lc = _last_close(p["st"], i - 1 if i >= 1 else i)
                if lc == lc:
                    mv += p["shares"] * lc
            for st in pending:
                if len(positions) >= MAXP:
                    break
                op = st["o"][i]
                if op != op or op <= 0:
                    continue
                room = cap_gross * eq_prev - mv      # 距总仓位上限的剩余空间
                if room <= 0:
                    break
                alloc = min(PER * eq_prev, cash, room)
                sh = int(alloc / (op * (1 + CB)) / 100) * 100
                if sh < 100:
                    continue
                cost = sh * op * (1 + CB)
                if cost > cash:
                    continue
                cash -= cost
                mv += sh * op
                positions.append({
                    "st": st, "code": st["code"], "name": st["name"],
                    "shares": sh, "entry": op, "spent": cost, "cost0": cost,
                    "realized": 0.0, "half": False, "peak": _last_close(st, i),
                    "iEntry": i, "inDate": cal[i],
                })
        pending = []

        # B) 用当日K线判定离场
        for p in list(positions):
            st = p["st"]
            c_i = st["c"][i]; h_i = st["h"][i]; l_i = st["l"][i]; o_i = st["o"][i]
            if c_i != c_i:
                continue
            stop = p["entry"] * (1 - STOP)
            if l_i == l_i and l_i <= stop:
                px = o_i if (o_i == o_i and o_i < stop) else stop
                close_full(p, i, px, "止损 -8%")
                positions.remove(p)
                continue
            if not p["half"] and h_i == h_i and h_i >= p["entry"] * (1 + TAKE):
                sell_part(p, i, p["entry"] * (1 + TAKE), 0.5)
                p["half"] = True
            if c_i > p["peak"]:
                p["peak"] = c_i
            m20 = st["ma20"][i]
            if c_i <= p["peak"] * (1 - TRAIL):
                close_full(p, i, c_i, "回撤 10%")
                positions.remove(p)
            elif m20 == m20 and c_i < m20:
                close_full(p, i, c_i, "跌破20日线")
                positions.remove(p)

        # B2) 大盘跌破20日线：把总仓位强制降到上限（浮盈最差者优先减仓）
        if i >= 1 and not idx_above[i] and positions:
            while positions:
                mv = 0.0
                for p in positions:
                    lc = _last_close(p["st"], i)
                    if lc == lc:
                        mv += p["shares"] * lc
                eq_now = cash + mv
                if eq_now <= 0 or mv / eq_now <= GDOWN:
                    break
                worst, wr = None, None
                for p in positions:
                    lc = _last_close(p["st"], i)
                    r = (lc / p["entry"] - 1) if (lc == lc and p["entry"]) else 0.0
                    if wr is None or r < wr:
                        wr, worst = r, p
                if worst is None:
                    break
                lc = _last_close(worst["st"], i)
                if lc != lc:
                    break
                close_full(worst, i, lc, "大盘跌破20日线·降仓")
                positions.remove(worst)

        # C) 收盘：生成次日买点（仅当沪深300 在 20 日线上方）
        if i >= 1 and idx_above[i]:
            slots = MAXP - len(positions)
            if slots > 0:
                cands = []
                for st in uni:
                    c_i = st["c"][i]
                    if c_i != c_i:
                        continue
                    m20 = st["ma20"][i]; m60 = st["ma60"][i]; vr = st["vr"][i]
                    if m20 != m20 or m60 != m60 or vr != vr:
                        continue
                    m20p = st["ma20"][i - 1]
                    if m20p != m20p:
                        continue
                    if c_i > m20 and m20 > m20p and m20 > m60 and vr > 1.0:
                        cands.append((vr, st))
                if cands:
                    cands.sort(key=lambda x: -x[0])
                    pending = [s for _, s in cands[:slots]]

        # D) 收盘估值
        eq = cash
        for p in positions:
            lc = _last_close(p["st"], i)
            if lc == lc:
                eq += p["shares"] * lc
        equity.append(eq)
        if eq > peak_eq:
            peak_eq = eq
        dd = (peak_eq - eq) / peak_eq if peak_eq > 0 else 0.0
        if dd > maxdd:
            maxdd = dd

    final_eq = equity[-1] if equity else CAP
    n = len(trades)
    wins = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] <= 0]
    gw = sum(t["pnl"] for t in wins)
    gl = -sum(t["pnl"] for t in losses)
    first = next((idx_close[k] for k in range(T) if idx_close[k] == idx_close[k]), None)
    last = next((idx_close[k] for k in range(T - 1, -1, -1) if idx_close[k] == idx_close[k]), None)
    stats = {
        "capital": CAP,
        "final": round(final_eq),
        "totalRet": (final_eq / CAP - 1) * 100,
        "annualRet": ((final_eq / CAP) ** (252.0 / T) - 1) * 100 if T else None,
        "maxDD": maxdd * 100,
        "trades": n,
        "winRate": (len(wins) / n * 100) if n else None,
        "profitFactor": (gw / gl) if gl > 0 else None,
        "avgWin": (gw / len(wins)) if wins else None,
        "avgLoss": (gl / len(losses)) if losses else None,
        "avgDays": (sum(t["days"] for t in trades) / n) if n else None,
        "openPos": len(positions),
        "benchRet": ((last / first - 1) * 100) if (first and last) else None,
        "start": cal[0] if T else None,
        "end": cal[-1] if T else None,
        "days": T,
    }
    step = max(1, T // 400)
    eq_curve = [round(equity[k]) for k in range(0, T, step)]
    idx_curve = [(round(idx_close[k] / first * CAP) if (first and idx_close[k] == idx_close[k]) else None)
                 for k in range(0, T, step)]
    return {"stats": stats, "equity": eq_curve, "bench": idx_curve,
            "trades": trades[-60:][::-1], "sample": len(uni)}


def main():
    t0 = time.time()
    print("[1/5] 拉取沪深主板股票名单...")
    lst = get_mainboard_list()
    print("      共 %d 只" % len(lst))
    if not lst:
        print("股票列表为空，终止。", file=sys.stderr)
        return 1

    print("[2/5] 探测K线数据源...")
    global KLINE_SOURCE, KLINE_HOSTS
    host = probe_kline_host()
    if host:
        KLINE_HOSTS = (host,)          # 锁定可用主机，避免每次请求重复探测被拦的域名
        KLINE_SOURCE = "tencent"
        src_label = "腾讯财经（前复权）"
    else:
        KLINE_SOURCE = "sina"
        src_label = "新浪财经（不复权·真实成交价）"
    print("      使用数据源：%s%s" % (src_label, "  [%s]" % host if host else ""))

    print("[3/5] 拉取指数K线（大盘环境 + 回测基准）...")
    idx_defs = [("sh000300", "000300", "沪深300"),
                ("sh000001", "000001", "上证指数"),
                ("sz399001", "399001", "深证成指")]
    idx_bars = {}
    for secid, code, nm in idx_defs:
        k = get_kline(secid, HIST)
        idx_bars[code] = k["bars"] if k else []

    # 以沪深300的交易日为全局日历
    cal = [b["d"] for b in idx_bars["000300"]]
    cal_pos = {d: i for i, d in enumerate(cal)}
    T = len(cal)

    indices, data_date, env_ok = [], None, False
    for secid, code, nm in idx_defs:
        bars = idx_bars[code]
        if len(bars) < 60:
            indices.append({"code": code, "name": nm, "close": None, "pct": None,
                            "ma20": None, "ma60": None, "above": None})
            continue
        closes = [b["c"] for b in bars]
        last = bars[-1]
        ma20, ma60 = avg(closes[-20:]), avg(closes[-60:])
        pct = (last["c"] / bars[-2]["c"] - 1) * 100
        above = last["c"] > ma20
        indices.append({"code": code, "name": nm, "close": round(last["c"], 2),
                        "pct": round(pct, 2), "ma20": round(ma20, 2),
                        "ma60": round(ma60, 2), "above": above})
        if code == "000300":
            env_ok = above
    data_date = cal[-1] if T else None
    print("      交易日历 %d 天（%s ~ %s）｜沪深300 在20日线上方: %s"
          % (T, cal[0] if T else "—", cal[-1] if T else "—", env_ok))

    print("[4/5] 并发拉取 %d 只股票K线（%d 根历史）并计算信号..." % (len(lst), HIST))
    stocks, uni, done = [], [], [0]
    lock = threading.Lock()

    def work(item):
        sym = item.get("symbol")
        if not sym:
            return None                      # 无代码：跳过，不算失败
        src_now = KLINE_SOURCE               # 记录本次取数所用口径
        k = get_kline(sym, HIST)
        with lock:
            done[0] += 1
            if done[0] % 200 == 0:
                print("      已发起 %d 次请求" % done[0])
        if not k or len(k["bars"]) < 70:
            return item                      # 取数失败：返回以便重试
        bars = k["bars"]
        a = analyze(bars[-70:], env_ok)
        if not a:
            return None
        a["code"] = item.get("code")
        a["name"] = item.get("name")
        a["src"] = "qfq" if src_now == "tencent" else "raw"   # 该股所用复权口径
        stocks.append(a)
        # 回测样本：记录口径，稍后只用同一种口径的数据做回测，避免复权口径混用
        if T:
            o, c, h, l, v, n = make_arrays(bars, T, cal_pos)
            if n >= 120:
                uni.append({"code": a["code"], "name": a["name"], "src": src_now,
                            "o": o, "c": c, "h": h, "l": l, "v": v})
        return None

    todo = list(lst)
    for pno, workers in ((1, 8), (2, 3), (3, 2)):
        if not todo:
            break
        print("      第 %d 轮：尝试 %d 只（并发 %d）" % (pno, len(todo), workers))
        fails = []
        with ThreadPoolExecutor(max_workers=workers) as ex:
            for r in ex.map(work, todo):
                if r is not None:
                    fails.append(r)
        todo = fails
        if todo:
            if KLINE_SOURCE == "tencent":
                # 中途可能被 WAF 再次封锁：重新探测主机，必要时切到新浪备用源
                newhost = probe_kline_host()
                if newhost:
                    KLINE_HOSTS = (newhost,)
                else:
                    print("      腾讯主机全部被拦截，切换新浪（不复权）源重试")
                    KLINE_SOURCE = "sina"
                    src_label = "新浪财经（不复权·真实成交价）"
            print("      本轮未取到 %d 只，冷却 15s 后重试..." % len(todo))
            time.sleep(15)

    hits = [s for s in stocks if s["all"]]
    n_qfq = sum(1 for s in stocks if s.get("src") == "qfq")
    n_raw = len(stocks) - n_qfq
    if n_raw == 0:
        src_label = "腾讯财经（前复权）"
    elif n_qfq == 0:
        src_label = "新浪财经（不复权·真实成交价）"
    else:
        src_label = ("混合口径：%d 只前复权(腾讯) + %d 只不复权(新浪·腾讯被WAF拦截时降级)"
                     % (n_qfq, n_raw))
    print("      有效 %d 只 / %d，命中买入条件 %d 只" % (len(stocks), len(lst), len(hits)))

    # ---------------- 回测 ----------------
    # 只用同一种复权口径的样本做回测：优先前复权（口径最严谨），前复权样本不足时退而用不复权并如实标注
    backtest = None
    uni_qfq = [s for s in uni if s["src"] == "tencent"]
    uni_raw = [s for s in uni if s["src"] != "tencent"]
    if len(uni_qfq) >= 300:
        uni_bt, basis = uni_qfq, "qfq"
    elif len(uni_raw) >= 300:
        uni_bt, basis = uni_raw, "raw"
    else:
        uni_bt, basis = (uni_qfq, "qfq") if len(uni_qfq) >= len(uni_raw) else (uni_raw, "raw")
    if T and uni_bt:
        print("      回测：计算均线/量比并逐日模拟（口径 %s，样本 %d 只）..."
              % ("前复权" if basis == "qfq" else "不复权", len(uni_bt)))
        for st in uni_bt:
            st["ma20"] = rolling_ma(st["c"], 20, T)
            st["ma60"] = rolling_ma(st["c"], 60, T)
            st["vr"] = rolling_vr(st["v"], 5, T)
        idx_close = array("d", [NAN]) * T
        for b in idx_bars["000300"]:
            p = cal_pos.get(b["d"])
            if p is not None:
                idx_close[p] = b["c"]
        idx_ma20 = rolling_ma(idx_close, 20, T)
        idx_above = [(idx_close[i] == idx_close[i] and idx_ma20[i] == idx_ma20[i]
                      and idx_close[i] > idx_ma20[i]) for i in range(T)]
        backtest = run_backtest(uni_bt, T, cal, idx_close, idx_above)
        backtest["basis"] = basis
        s = backtest["stats"]
        print("      回测样本 %d 只 ｜ 交易 %d 笔 ｜ 总收益 %s%% ｜ 最大回撤 %s%%"
              % (backtest["sample"], s["trades"],
                 ("%.1f" % s["totalRet"]) if s["totalRet"] is not None else "—",
                 ("%.1f" % s["maxDD"]) if s["maxDD"] is not None else "—"))
    else:
        print("      ! 指数或个股历史不足，跳过回测")

    market = {
        "generatedAt": time.strftime("%Y-%m-%d %H:%M:%S"),
        "dataDate": data_date,
        "envOk": env_ok,
        "indices": indices,
        "stocks": stocks,
        "scanned": len(lst),
        "valid": len(stocks),
        "source": src_label,
        "backtest": backtest,
    }

    print("[5/5] 生成HTML...")
    tpl = HTML_TEMPLATE.replace("/*__DATA__*/null",
                                json.dumps(market, ensure_ascii=False, separators=(",", ":")))
    with open(OUT_HTML, "w", encoding="utf-8") as f:
        f.write(tpl)
    print("完成 -> %s  （耗时 %.0fs）" % (OUT_HTML, time.time() - t0))
    return 0


HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>A股 · 规则执行终端</title>
<style>
  :root{--bg:#0a0d12;--panel:#11161f;--panel2:#0d1219;--line:#1e2632;--text:#c9d1d9;
    --dim:#7d8794;--dim2:#55606e;--up:#ef4444;--down:#22c55e;--amber:#f59e0b;--blue:#4c9aff;}
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--text);
    font-family:-apple-system,"Segoe UI","Microsoft YaHei",system-ui,sans-serif;font-size:14px;
    -webkit-font-smoothing:antialiased;}
  .mono{font-family:"JetBrains Mono",Consolas,"Courier New",monospace;font-variant-numeric:tabular-nums}
  .wrap{max-width:1300px;margin:0 auto;padding:22px 20px 60px}
  header{display:flex;align-items:flex-end;justify-content:space-between;gap:16px;flex-wrap:wrap;margin-bottom:18px}
  .brand{display:flex;align-items:center;gap:12px}
  .brand .bar{width:4px;height:34px;background:var(--blue);border-radius:2px}
  h1{font-size:19px;margin:0;letter-spacing:.5px}
  .sub{font-size:12px;color:var(--dim);margin-top:4px}
  .stamp{font-size:12px;color:var(--dim);text-align:right;line-height:1.7}
  .card{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:16px 18px;margin-bottom:16px}
  .card h2{font-size:12px;letter-spacing:2px;color:var(--dim);margin:0 0 14px;display:flex;align-items:center;gap:9px;font-weight:600}
  .card h2::before{content:"";width:3px;height:13px;background:var(--blue);border-radius:2px}
  .envTop{display:flex;align-items:center;gap:14px;flex-wrap:wrap;margin-bottom:14px}
  .light{display:inline-flex;align-items:center;gap:9px;padding:8px 14px;border-radius:8px;font-weight:600;font-size:14px;border:1px solid}
  .light .dot{width:9px;height:9px;border-radius:50%}
  .light.go{color:var(--up);border-color:rgba(239,68,68,.45);background:rgba(239,68,68,.10)}
  .light.go .dot{background:var(--up);box-shadow:0 0 10px var(--up)}
  .light.stop{color:var(--down);border-color:rgba(34,197,94,.45);background:rgba(34,197,94,.10)}
  .light.stop .dot{background:var(--down);box-shadow:0 0 10px var(--down)}
  .tip{font-size:12px;color:var(--dim)}
  .idxGrid{display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:12px}
  .idx{background:var(--panel2);border:1px solid var(--line);border-radius:8px;padding:12px 14px}
  .idx .nm{font-size:12px;color:var(--dim);display:flex;justify-content:space-between}
  .idx .px{font-size:25px;margin:7px 0 4px;font-weight:600}
  .idx .meta{font-size:12px;color:var(--dim);display:flex;gap:12px;flex-wrap:wrap}
  .tagOK{color:var(--up)} .tagNO{color:var(--down)}
  .inputRow{display:flex;gap:10px;flex-wrap:wrap;align-items:center}
  input[type=text]{background:var(--panel2);border:1px solid var(--line);color:var(--text);
    padding:11px 14px;border-radius:8px;font-size:15px;width:230px;outline:none;
    font-family:"JetBrains Mono",Consolas,monospace;}
  input[type=text]:focus{border-color:var(--blue)}
  button{background:var(--blue);color:#fff;border:0;padding:11px 20px;border-radius:8px;
    font-size:14px;font-weight:600;cursor:pointer;transition:.15s}
  button:hover{filter:brightness(1.12)}
  button.ghost{background:transparent;border:1px solid var(--line);color:var(--text)}
  .hint{font-size:12px;color:var(--dim2);margin-top:10px}
  .verdict{margin-top:16px;display:none}
  .vhead{display:flex;align-items:center;gap:14px;flex-wrap:wrap;padding-bottom:14px;border-bottom:1px dashed var(--line)}
  .vbadge{padding:7px 16px;border-radius:8px;font-weight:700;font-size:15px;border:1px solid}
  .vbadge.buy{color:var(--up);border-color:rgba(239,68,68,.5);background:rgba(239,68,68,.12)}
  .vbadge.hold{color:var(--amber);border-color:rgba(245,158,11,.5);background:rgba(245,158,11,.12)}
  .vbadge.out{color:var(--down);border-color:rgba(34,197,94,.5);background:rgba(34,197,94,.12)}
  .vname{font-size:16px;font-weight:600}
  .vname span{color:var(--dim);font-size:13px;margin-left:8px;font-weight:400}
  .vcols{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:22px;margin-top:14px}
  .vcols h3{font-size:12px;letter-spacing:1px;color:var(--dim);margin:0 0 10px;font-weight:600}
  .cond{display:flex;justify-content:space-between;gap:10px;padding:6px 0;border-bottom:1px solid #161c26;font-size:13px}
  .cond .m{color:#e6edf3} .cond .x{color:var(--dim2)}
  .lv{display:flex;justify-content:space-between;padding:7px 0;border-bottom:1px solid #161c26;font-size:13px}
  .scanBar{display:flex;align-items:center;gap:14px;flex-wrap:wrap}
  .scanStat{font-size:12px;color:var(--dim)}
  table{width:100%;border-collapse:collapse;margin-top:14px;font-size:13px}
  th{font-size:11px;letter-spacing:.6px;color:var(--dim);text-align:right;padding:8px 10px;
    border-bottom:1px solid var(--line);font-weight:600;white-space:nowrap}
  th:first-child,td:first-child,th:nth-child(2),td:nth-child(2){text-align:left}
  td{padding:9px 10px;border-bottom:1px solid #161c26;text-align:right;white-space:nowrap;
    font-family:"JetBrains Mono",Consolas,monospace}
  tbody tr:hover{background:#141a24}
  .empty{padding:22px;text-align:center;color:var(--dim);font-size:13px;line-height:1.8}
  .secTtl{font-size:12px;letter-spacing:1px;color:var(--dim);margin:18px 0 0;font-weight:600}
  footer{margin-top:26px;border-top:1px solid var(--line);padding-top:18px;font-size:12px;color:var(--dim);line-height:1.9}
  footer b{color:var(--text)}
  .ruleGrid{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:14px;margin-bottom:16px}
  .rule{background:var(--panel2);border:1px solid var(--line);border-radius:8px;padding:12px 14px}
  .rule h4{margin:0 0 8px;font-size:13px;color:#e6edf3}
  .rule ul{margin:0;padding-left:16px} .rule li{margin:3px 0}
  .dis{color:var(--dim2);border-top:1px dashed var(--line);padding-top:12px}
  .btGrid{display:grid;grid-template-columns:repeat(auto-fit,minmax(148px,1fr));gap:12px;margin-bottom:16px}
  .btm{background:var(--panel2);border:1px solid var(--line);border-radius:8px;padding:12px 14px}
  .btm .k{font-size:11px;color:var(--dim);letter-spacing:.5px}
  .btm .v{font-size:21px;font-weight:600;margin-top:5px;font-family:"JetBrains Mono",Consolas,monospace}
  .btm .s{font-size:11px;color:var(--dim2);margin-top:3px}
  .chartBox{background:var(--panel2);border:1px solid var(--line);border-radius:8px;padding:12px 10px 6px;margin-bottom:16px}
  .legend{display:flex;gap:18px;font-size:12px;color:var(--dim);margin:0 0 6px 6px;flex-wrap:wrap}
  .legend i{display:inline-block;width:14px;height:3px;border-radius:2px;vertical-align:middle;margin-right:6px}
  .assume{font-size:12px;color:var(--dim2);line-height:1.9;margin-top:14px;border-top:1px dashed var(--line);padding-top:12px}
  .assume b{color:var(--dim)}
</style>
</head>
<body>
<div class="wrap">
  <header>
    <div class="brand">
      <div class="bar"></div>
      <div>
        <h1>A股 · 规则执行终端</h1>
        <div class="sub">趋势跟随系统 v1 &nbsp;|&nbsp; 范围：沪市主板 + 深市主板（已排除科创板/ST） &nbsp;|&nbsp; 取不到即为 “—”</div>
      </div>
    </div>
    <div class="stamp">
      <div id="stDataDate">数据截至 —</div>
      <div id="stGen">—</div>
      <div id="stSrc">—</div>
    </div>
  </header>

  <section class="card">
    <h2>一、大盘环境（决定今天能不能进场）</h2>
    <div class="envTop" id="envTop"></div>
    <div class="idxGrid" id="idxGrid"></div>
  </section>

  <section class="card">
    <h2>二、个股诊断（现在该买、该持、还是该卖）</h2>
    <div class="inputRow">
      <input type="text" id="codeInput" placeholder="输入 6 位股票代码，如 600519" maxlength="6">
      <button id="diagBtn">诊断</button>
      <button class="ghost" id="clearBtn">清空</button>
    </div>
    <div class="hint">规则：站上20日线 + 20日线上扬 + 20日线&gt;60日线 + 放量(量比&gt;1) + 大盘在20日线上方 → 才判 “可买入”，缺一即降级。可诊断范围＝沪市主板 + 深市主板（已排除科创板、创业板、ST）。</div>
    <div class="verdict" id="verdict"></div>
  </section>

  <section class="card">
    <h2>三、沪深主板 全扫描（有哪些符合买入条件）</h2>
    <div class="scanBar">
      <div class="scanStat" id="scanStat">—</div>
    </div>
    <div id="scanResult"></div>
  </section>

  <section class="card">
    <h2>四、策略回测（历史验证）</h2>
    <div id="btWrap"></div>
  </section>

  <section class="card">
    <h2>五、我的持仓管理（记下成本，系统每天替你盯盘）</h2>
    <div class="inputRow">
      <input type="text" id="hCode" placeholder="6 位代码" maxlength="6">
      <input type="text" id="hCost" placeholder="买入成本价" maxlength="10">
      <input type="text" id="hQty" placeholder="股数（可空）" maxlength="9">
      <button id="hAdd">加入持仓</button>
      <button class="ghost" id="hClear">清空全部</button>
    </div>
    <div class="hint">持仓只保存在你本机浏览器（localStorage），不上传任何服务器。成本价填你的实际买入均价即可。判定规则与第二区块一致：跌破成本−8% 或 收盘跌破20日线 → 卖出；达 +20% → 减半仓；其余 → 持有。</div>
    <div id="holdings"></div>
  </section>

  <footer>
    <div class="ruleGrid">
      <div class="rule"><h4>买入（需全部满足）</h4><ul>
        <li>收盘价 &gt; 20日均线</li><li>20日均线 向上</li>
        <li>20日均线 &gt; 60日均线</li><li>当日成交量 &gt; 5日均量（放量）</li>
        <li>沪深300 在其20日均线上方</li><li>单只仓位 ≤ 总资金 20%</li>
      </ul></div>
      <div class="rule"><h4>卖出（触发任一）</h4><ul>
        <li>跌破成本 −8%（铁律止损）</li><li>+20% 减半仓，余仓转移动止损</li>
        <li>从最高点回撤 10%</li><li>收盘跌破 20日均线</li>
      </ul></div>
      <div class="rule"><h4>仓位与风控</h4><ul>
        <li>大盘在均线上 → 最多 7 成仓</li><li>大盘跌破均线 → 降至 3 成或空仓</li>
        <li>单笔最大亏损 ≤ 总资金 1–2%</li><li>同时持有 ≤ 5 只；不融资、不加杠杆</li>
      </ul></div>
    </div>
    <div class="dis">
      本工具为交易规则的机械化执行辅助，<b>不构成任何投资建议</b>，不预测涨跌。全部数值由公开行情抓取并计算，取不到数据一律显示 “—”，不含任何模拟或填充数据。
      数据为生成时的快照；重跑 build_stock_tool.py 即可刷新到最新交易日。股市有风险，请先用小资金验证规则、严格执行止损。
      <br>均线口径：右上角标注 “腾讯财经（前复权）” 时按前复权价计算；若标注 “混合口径”，说明腾讯接口部分时段被风控拦截，被拦的那部分个股改用新浪<b>不复权</b>真实成交价计算（除权除息日会有价格跳空），具体每只股票的口径见第二区块“个股诊断”。
      <br>回测口径：优先用<b>前复权</b>长历史（约3.2年）逐日模拟；若腾讯前复权源被风控拦截，则降级为<b>不复权</b>并与该口径样本做回测（第四节标注实际口径）。均含交易成本；历史统计<b>不代表未来收益</b>，参数存在过拟合风险。
    </div>
  </footer>
</div>

<script>
const M = /*__DATA__*/null;
const UP='#ef4444', DOWN='#22c55e', FLAT='#8b949e';
const fmt=(n,d=2)=>(n===null||n===undefined||isNaN(n))?'—':Number(n).toFixed(d);
const pctStr=p=>(p===null||p===undefined||isNaN(p))?'—':((p>0?'+':'')+p.toFixed(2)+'%');
const pctColor=p=>(p===null||p===undefined||isNaN(p))?FLAT:(p>0?UP:p<0?DOWN:FLAT);

function renderEnv(){
  const box=document.getElementById('envTop');
  const on=M.envOk;
  box.innerHTML='<div class="light '+(on?'go':'stop')+'"><span class="dot"></span>'
    +(on?'环境偏多 · 可进场':'环境偏空 · 建议空仓/轻仓')+'</div>'
    +'<span class="tip">'+(on?'沪深300 在20日均线上方 → 允许按规则建仓，总仓位上限7成'
      :'沪深300 跌破20日均线 → 暂停买入，仓位降至3成以下')+'</span>';
  const g=document.getElementById('idxGrid');
  if(!M.indices||!M.indices.length){ g.innerHTML='<div class="empty">无指数数据</div>'; return; }
  g.innerHTML=M.indices.map(d=>{
    const above=d.above;
    return '<div class="idx"><div class="nm"><span>'+d.name+'</span><span>'+d.code+'</span></div>'
      +'<div class="px" style="color:'+pctColor(d.pct)+'">'+fmt(d.close)+'</div>'
      +'<div class="meta"><span style="color:'+pctColor(d.pct)+'">'+pctStr(d.pct)+'</span>'
      +'<span>20线 <b>'+fmt(d.ma20)+'</b></span>'
      +'<span>'+(above===null||above===undefined?'—':(above?'<b class="tagOK">线上</b>':'<b class="tagNO">线下</b>'))+'</span>'
      +'</div></div>';
  }).join('');
  document.getElementById('stDataDate').textContent='数据截至 '+(M.dataDate||'—');
  document.getElementById('stGen').textContent='生成于 '+(M.generatedAt||'—');
  document.getElementById('stSrc').textContent=M.source||'—';
}

function condRow(label,pass){
  return '<div class="cond"><span class="'+(pass?'m':'x')+'">'+(pass?'✓':'✗')+' '+label+'</span>'
    +'<span class="'+(pass?'m':'x')+'">'+(pass?'满足':'不满足')+'</span></div>';
}

function diagnose(){
  const box=document.getElementById('verdict');
  const code=(document.getElementById('codeInput').value||'').trim();
  box.style.display='block';
  if(!/^\d{6}$/.test(code)){ box.innerHTML='<div class="empty">请输入 6 位股票代码</div>'; return; }
  const s=(M.stocks||[]).find(x=>x.code===code);
  if(!s){ box.innerHTML='<div class="empty">该代码不在扫描池内，或无有效K线数据。<br>（本系统的可交易范围＝沪市主板 + 深市主板，已排除科创板、创业板、ST）</div>'; return; }
  let badge,cls;
  if(s.all){badge='可买入';cls='buy';}
  else if(s.c.aboveMA20&&s.c.ma20gt60){badge='持有观察（不新买）';cls='hold';}
  else {badge='离场 / 不买';cls='out';}
  const stop=s.close*0.92, take=s.close*1.20, trail=s.high20*0.90;
  box.innerHTML=''
    +'<div class="vhead"><div class="vbadge '+cls+'">'+badge+'</div>'
    +'<div class="vname">'+s.name+' <span>'+code+'</span></div>'
    +'<div style="color:'+pctColor(s.pct)+';font-size:18px;font-family:monospace">'+fmt(s.close)
    +' <span style="font-size:13px">'+pctStr(s.pct)+'</span></div>'
    +'<div style="color:var(--dim);font-size:12px">截至 '+s.lastDate
    +' · 口径：'+(s.src==='qfq'?'前复权':'不复权')+'</div></div>'
    +'<div class="vcols"><div><h3>买入条件核对</h3>'
    +condRow('收盘价 &gt; 20日均线 ('+fmt(s.ma20)+')',s.c.aboveMA20)
    +condRow('20日均线拐头向上',s.c.ma20Up)
    +condRow('20日均线 &gt; 60日均线 ('+fmt(s.ma60)+')',s.c.ma20gt60)
    +condRow('放量：量比 '+fmt(s.volRatio,2)+' &gt; 1',s.c.volSurge)
    +condRow('大盘（沪深300）在20日线上',!!M.envOk)
    +'</div><div><h3>关键价位（按现价测算）</h3>'
    +'<div class="lv"><span>现价</span><b>'+fmt(s.close)+'</b></div>'
    +'<div class="lv"><span>止损位（−8%）</span><b style="color:'+DOWN+'">'+fmt(stop)+'</b></div>'
    +'<div class="lv"><span>止盈位（+20%）</span><b style="color:'+UP+'">'+fmt(take)+'</b></div>'
    +'<div class="lv"><span>移动止损（20日最高回撤10%）</span><b>'+fmt(trail)+'</b></div>'
    +'<div class="lv"><span>20日均线（跌破即离场）</span><b>'+fmt(s.ma20)+'</b></div>'
    +'<div class="lv"><span>建议单只仓位上限</span><b>总资金 20%</b></div>'
    +'</div></div>';
}

function stockTable(rows){
  let h='<table><thead><tr><th>代码</th><th>名称</th><th>现价</th><th>涨跌幅</th><th>20日均线</th><th>距20线</th><th>量比</th><th>止损位(-8%)</th><th>止盈位(+20%)</th></tr></thead><tbody>';
  for(const s of rows){
    const dist=(s.ma20!=null&&s.close!=null)?(s.close/s.ma20-1)*100:null;
    h+='<tr><td>'+s.code+'</td><td>'+s.name+'</td>'
      +'<td style="color:'+pctColor(s.pct)+'">'+fmt(s.close)+'</td>'
      +'<td style="color:'+pctColor(s.pct)+'">'+pctStr(s.pct)+'</td>'
      +'<td>'+fmt(s.ma20)+'</td>'
      +'<td style="color:'+pctColor(dist)+'">'+(dist===null?'—':pctStr(dist))+'</td>'
      +'<td>'+fmt(s.volRatio,2)+'</td>'
      +'<td style="color:'+DOWN+'">'+fmt(s.close*0.92)+'</td>'
      +'<td style="color:'+UP+'">'+fmt(s.close*1.20)+'</td></tr>';
  }
  return h+'</tbody></table>';
}

function renderScan(){
  const all=M.stocks||[];
  const hits=all.filter(s=>s.all).sort((a,b)=>(b.pct||0)-(a.pct||0));
  const watch=all.filter(s=>s.stockOk&&!s.all).sort((a,b)=>(b.pct||0)-(a.pct||0));
  document.getElementById('scanStat').innerHTML=
    '扫描 '+M.scanned+' 只 ｜ 有效 '+M.valid+' 只 ｜ <b style="color:'+UP+'">可买入 '+hits.length+' 只</b>'
    +' ｜ 观察名单 '+watch.length+' 只'
    +(M.envOk?'':' <span style="color:'+DOWN+'">（当前大盘偏空 → 一律不进场）</span>');
  const out=document.getElementById('scanResult');
  let html='';
  if(hits.length){
    html+='<div class="secTtl" style="color:'+UP+'">✓ 可买入（全部条件满足）</div>'+stockTable(hits);
  }else{
    html+='<div class="empty">今天没有股票同时满足全部买入条件。<br>空仓等待也是这套系统的一部分——强行买入才是亏损的开始。</div>';
  }
  if(watch.length){
    html+='<div class="secTtl">观察名单：个股条件已满足，仅差“大盘在20日线上方”一项</div>'+stockTable(watch);
  }
  out.innerHTML=html;
}

// ---------------- 回测 ----------------
function btCard(k,v,col,s){
  return '<div class="btm"><div class="k">'+k+'</div>'
    +'<div class="v" style="color:'+(col||'#e6edf3')+'">'+v+'</div>'
    +(s?'<div class="s">'+s+'</div>':'')+'</div>';
}
function chartSVG(eq,bench){
  const W=1180,H=280,P=14;
  const all=[];
  for(const v of eq||[]) if(v!=null) all.push(v);
  for(const v of bench||[]) if(v!=null) all.push(v);
  if(!all.length) return '<div class="empty">无曲线数据</div>';
  let lo=Math.min.apply(null,all), hi=Math.max.apply(null,all);
  if(hi<=lo) hi=lo+1;
  const nb=Math.max(1,(eq||[]).length-1);
  const X=i=>P+i/nb*(W-2*P);
  const Y=v=>H-P-(v-lo)/(hi-lo)*(H-2*P);
  const path=a=>{let d='',st=false;for(let i=0;i<a.length;i++){const v=a[i];if(v==null)continue;
    d+=(st?'L':'M')+X(i).toFixed(1)+' '+Y(v).toFixed(1)+' ';st=true;}return d;};
  const base=Y(1000000);
  return '<svg viewBox="0 0 '+W+' '+H+'" style="width:100%;height:280px;display:block">'
    +'<line x1="'+P+'" y1="'+base+'" x2="'+(W-P)+'" y2="'+base+'" stroke="#2a3441" stroke-dasharray="4 4"/>'
    +'<text x="'+(W-P)+'" y="'+(base-5)+'" fill="#55606e" font-size="11" text-anchor="end">初始 100 万</text>'
    +'<path d="'+path(bench)+'" fill="none" stroke="#7d8794" stroke-width="1.4"/>'
    +'<path d="'+path(eq)+'" fill="none" stroke="#4c9aff" stroke-width="2"/>'
    +'</svg>';
}
function renderBt(){
  const box=document.getElementById('btWrap');
  const B=M.backtest;
  if(!B){ box.innerHTML='<div class="empty">本次构建未执行回测（指数历史或个股数据不足）。</div>'; return; }
  const s=B.stats;
  const hasBasis = (B.basis==='qfq');
  let h='<div class="btGrid">';
  h+=btCard('总收益率',pctStr(s.totalRet),pctColor(s.totalRet),s.start+' ~ '+s.end);
  h+=btCard('年化收益',s.annualRet==null?'—':pctStr(s.annualRet),pctColor(s.annualRet),s.days+' 个交易日');
  h+=btCard('最大回撤',s.maxDD==null?'—':('−'+fmt(s.maxDD)+'%'),'#f59e0b','从最高点回落');
  h+=btCard('胜率',s.winRate==null?'—':(fmt(s.winRate)+'%'),'#e6edf3',(s.trades||0)+' 笔已平仓');
  h+=btCard('盈亏比',s.profitFactor==null?'—':fmt(s.profitFactor),'#e6edf3','总盈利 / 总亏损');
  h+=btCard('平均持仓',s.avgDays==null?'—':(fmt(s.avgDays,0)+' 天'),'#e6edf3','交易日');
  h+=btCard('期末权益',fmt(s.final,0),'#e6edf3','初始 '+fmt(s.capital,0)+' 元');
  h+=btCard('同期沪深300',s.benchRet==null?'—':pctStr(s.benchRet),pctColor(s.benchRet),'基准对比');
  h+=btCard('回测口径',hasBasis?'前复权':'不复权',hasBasis?'#e6edf3':'#f59e0b',hasBasis?'口径一致':'源被拦截后降级');
  h+='</div>';
  h+='<div class="chartBox"><div class="legend">'
    +'<span><i style="background:#4c9aff"></i>策略资金曲线（初始100万）</span>'
    +'<span><i style="background:#7d8794"></i>沪深300（同期，同起点）</span></div>'
    +chartSVG(B.equity,B.bench)+'</div>';
  if(B.trades&&B.trades.length){
    h+='<div class="secTtl">最近成交明细（最新在前，最多 60 笔）</div>';
    h+='<table><thead><tr><th>代码</th><th>名称</th><th>买入日</th><th>卖出日</th><th>买入价</th><th>卖出价</th><th>收益</th><th>盈亏(元)</th><th>持仓</th><th>出场原因</th></tr></thead><tbody>';
    for(const t of B.trades){
      h+='<tr><td>'+t.code+'</td><td>'+t.name+'</td><td>'+t.inDate+'</td><td>'+t.outDate+'</td>'
        +'<td>'+fmt(t.inPx)+'</td><td>'+fmt(t.outPx)+'</td>'
        +'<td style="color:'+pctColor(t.ret)+'">'+(t.ret==null?'—':pctStr(t.ret))+'</td>'
        +'<td style="color:'+(t.pnl>0?UP:t.pnl<0?DOWN:FLAT)+'">'+fmt(t.pnl,0)+'</td>'
        +'<td>'+t.days+' 日</td><td style="text-align:left">'+t.reason+'</td></tr>';
    }
    h+='</tbody></table>';
  }else{
    h+='<div class="empty">回测区间内未触发任何交易。</div>';
  }
  h+='<div class="assume"><b>回测假设（请知悉口径）：</b>'
    +'样本 '+B.sample+' 只沪深主板个股 ｜ '
    +(hasBasis
      ? '均线/量比按<b>前复权</b>价计算，复权口径一致；'
      : '均线/量比按<b>不复权</b>真实成交价计算（本次腾讯前复权源被风控拦截，口径降级；除权除息日存在价格跳空，结果仅供参考）；')
    +'信号在<b>收盘</b>确认、<b>次日开盘</b>买入（避免用未来数据）；'
    +'买入成本含佣金 0.03%，卖出含佣金+印花税 0.08%；'
    +'止损按当日最低价触发、成交价取 −8% 止损位（若跳空低开则按开盘价）；'
    +'“+20% 减半”按当日最高价触及执行；移动止损与跌破20日线按收盘价执行；'
    +'大盘（沪深300）在20日线上方时总仓位≤70%，跌破20日线时<b>停止开新仓并把总仓位降至≤30%</b>（浮盈最差者优先减仓）；'
    +'同一天多个买点时按量比从高到低优先买入；单笔最大买入 20% 仓位、最多同时持有 5 只。'
    +'<br>以上为历史统计，<b>不代表未来收益</b>；未计滑点、停牌与涨跌停无法成交等情形，实际结果会偏差。</div>';
  box.innerHTML=h;
}

// ---------------- 持仓管理 ----------------
const HKEY='astock_holdings_v1';
function loadHoldings(){try{return JSON.parse(localStorage.getItem(HKEY))||[];}catch(e){return [];}}
function saveHoldings(a){try{localStorage.setItem(HKEY,JSON.stringify(a));}catch(e){}}
function addHolding(){
  const code=(document.getElementById('hCode').value||'').trim();
  const cost=parseFloat(document.getElementById('hCost').value);
  const qty=parseFloat(document.getElementById('hQty').value);
  if(!/^\d{6}$/.test(code)){alert('请输入 6 位股票代码');return;}
  if(!(cost>0)){alert('请输入有效的买入成本价');return;}
  const a=loadHoldings();
  if(a.some(x=>x.code===code)){alert('该代码已在持仓中');return;}
  a.push({code:code,cost:cost,qty:(qty>0?qty:null)});
  saveHoldings(a);
  ['hCode','hCost','hQty'].forEach(id=>{document.getElementById(id).value='';});
  renderHoldings();
}
function removeHolding(code){
  saveHoldings(loadHoldings().filter(x=>x.code!==code));
  renderHoldings();
}
function renderHoldings(){
  const box=document.getElementById('holdings');
  const a=loadHoldings();
  if(!a.length){box.innerHTML='<div class="empty">还没有持仓。把你的买入成本填进来，系统会每天算出“该拿还是该走”。</div>';return;}
  let h='<table><thead><tr><th>代码</th><th>名称</th><th>成本</th><th>现价</th><th>盈亏</th><th>止损位(-8%)</th><th>止盈位(+20%)</th><th>20日线</th><th>建议动作</th><th></th></tr></thead><tbody>';
  for(const x of a){
    const s=(M.stocks||[]).find(y=>y.code===x.code);
    const del='<button class="ghost" style="padding:4px 10px;font-size:12px" onclick="removeHolding(\''+x.code+'\')">删</button>';
    if(!s){
      h+='<tr><td>'+x.code+'</td><td colspan="8" style="text-align:left;color:var(--dim)">不在扫描池内或无有效K线（仅支持沪深主板，取不到即为 “—”）</td><td>'+del+'</td></tr>';
      continue;
    }
    const cur=s.close, ret=(cur/x.cost-1)*100;
    const stop=x.cost*0.92, take=x.cost*1.20;
    let act,cls;
    if(cur<=stop){act='触发止损 → 卖出';cls='out';}
    else if(cur<s.ma20){act='跌破20日线 → 卖出';cls='out';}
    else if(cur>=take){act='达标+20% → 减半仓';cls='buy';}
    else {act='持有';cls='hold';}
    h+='<tr><td>'+s.code+'</td><td>'+s.name+'</td>'
      +'<td>'+fmt(x.cost)+'</td>'
      +'<td style="color:'+pctColor(ret)+'">'+fmt(cur)+'</td>'
      +'<td style="color:'+pctColor(ret)+'">'+pctStr(ret)+'</td>'
      +'<td style="color:'+DOWN+'">'+fmt(stop)+'</td>'
      +'<td style="color:'+UP+'">'+fmt(take)+'</td>'
      +'<td>'+fmt(s.ma20)+'</td>'
      +'<td><span class="vbadge '+cls+'" style="padding:3px 10px;font-size:12px">'+act+'</span></td>'
      +'<td>'+del+'</td></tr>';
  }
  h+='</tbody></table><div class="hint">按最近收盘价 s.close 与20日均线判定（更准确的判断以当日收盘后重跑工具为准）。</div>';
  box.innerHTML=h;
}

document.getElementById('diagBtn').addEventListener('click',diagnose);
document.getElementById('clearBtn').addEventListener('click',function(){
  document.getElementById('codeInput').value='';
  document.getElementById('verdict').style.display='none';
});
document.getElementById('codeInput').addEventListener('keydown',function(e){if(e.key==='Enter')diagnose();});
document.getElementById('hAdd').addEventListener('click',addHolding);
document.getElementById('hCode').addEventListener('keydown',function(e){if(e.key==='Enter')addHolding();});
document.getElementById('hCost').addEventListener('keydown',function(e){if(e.key==='Enter')addHolding();});
document.getElementById('hClear').addEventListener('click',function(){
  if(!loadHoldings().length) return;
  if(confirm('确定清空全部持仓记录？')){saveHoldings([]);renderHoldings();}
});
renderEnv(); renderScan(); renderBt(); renderHoldings();
</script>
</body>
</html>
"""

if __name__ == "__main__":
    sys.exit(main())