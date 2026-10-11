#!/usr/bin/env python3
"""Binance USDT-M 무기한 선물 펌핑 이벤트 스캐너.

기본 필터 (절대 기준)
- 1시간봉, 연속 24시간 롤링 윈도우로 최근 N일 스캔
- 24h 거래대금 >= MIN_QUOTE_VOL AND (종가 또는 고가 상승률 >= MIN_GAIN)
- 48시간 이내로 이어지는 히트는 하나의 이벤트로 병합

논문 기반 보강
- Kamps & Kleinberg (2018) "To the Moon":
  절대 임계값 대신 '직전 기준선 대비' 거래량/가격 급변으로 펌핑을 정의.
  → VolX: 24h 거래대금 / 직전 7일 평균 24h 거래대금 (--min-vol-ratio 로 필터 가능)
- Xu & Livshits (2019) "Anatomy of a Pump-and-Dump":
  급등 전 신호(선행 수익률, 선행 거래량 증가)와 이후 덤프로 펌프앤덤프를 식별.
  → Pre72h: 이벤트 직전 72h 수익률, PreVolX: 직전 24h 거래대금 / 그 이전 7일 평균
  → DD72h: 피크 이후 72h 내 최대 낙폭 (덤프 여부)
- He, Manela, Ross, von Wachter (2022) "Fundamentals of Perpetual Futures":
  펀딩비는 선물-현물 괴리(premium)를 되돌리는 장치 → 펀딩비와 함께 괴리를 직접 확인.
  → MinPrem: 이벤트 중 최저 premium index (mark vs index, 음수 = 선물 디스카운트)
- 이벤트별 피크 OI / OI 증가율(openInterestHist, 최근 30일만) / 최저 펀딩비

v3 추가 기능
- 음펀비/프리미엄 필터: --max-prem (최저 premium index 상한), --max-funding (최저 funding rate 상한)
- 조기경보 라이브 모드: --live (최근 24h/72h 거래량 급증 및 상승 모멘텀 실시간 포착)
"""
import argparse
import asyncio
import time
from datetime import datetime, timezone

import aiohttp

BASE = "https://fapi.binance.com"
HOUR_MS = 3_600_000
WINDOW = 24                 # 롤링 윈도우 (h)
BASELINE = 168              # 기준선 (h) = 7일
PRE = 72                    # 선행 신호 구간 (h)
POST = 72                   # 덤프 측정 구간 (h)
LOOKBACK = BASELINE + PRE   # 스캔 시작 전 추가로 받아올 봉 수
LOOKBACK_LIVE = BASELINE + PRE + WINDOW  # 라이브 모드 필요 봉 수 (264h)
MERGE_GAP_MS = 48 * HOUR_MS
OI_LIMIT_MS = 30 * 24 * HOUR_MS
CONCURRENCY = 15            # 안전한 동시 요청 수 (113개 종목 고속 스캔)


DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/json",
}


class Client:
    def __init__(self, session: aiohttp.ClientSession, concurrency: int):
        self.s = session
        self.sem = asyncio.Semaphore(concurrency)

    async def get(self, path, params=None, retries=5):
        for attempt in range(retries):
            async with self.sem:
                try:
                    async with self.s.get(BASE + path, params=params,
                                          headers=DEFAULT_HEADERS,
                                          timeout=aiohttp.ClientTimeout(total=30)) as r:
                        # 1분 누적 가중치 헤더 감지 및 선제적 백오프
                        used_weight = r.headers.get("x-mbx-used-weight-1m")
                        if used_weight:
                            try:
                                uw = int(used_weight)
                                if uw >= 2000:
                                    await asyncio.sleep(4)
                                elif uw >= 1700:
                                    await asyncio.sleep(1)
                            except ValueError:
                                pass

                        if r.status in (429, 418):
                            body_text = await r.text()
                            retry_after = int(r.headers.get("Retry-After", 0))
                            wait = max(retry_after, 30 * (attempt + 1))
                            
                            # 418 밴 잔여 시간 파싱 (예: banned until 1791493196751)
                            if "banned until" in body_text:
                                import re
                                m = re.search(r"banned until (\d+)", body_text)
                                if m:
                                    ban_until_ms = int(m.group(1))
                                    now_ms = int(time.time() * 1000)
                                    rem_sec = max(5, int((ban_until_ms - now_ms) / 1000) + 2)
                                    print(f"\n[rate-limit] 바이낸스 IP 임시 차단 감지 (해제까지 {rem_sec//60}분 {rem_sec%60}초 남음). 대기합니다...", flush=True)
                                    wait = rem_sec

                            print(f"\n[rate-limit] HTTP {r.status} on {path}. Cooling down {wait}s...", flush=True)
                            await asyncio.sleep(wait)
                            continue
                        if r.status >= 500:
                            await asyncio.sleep(2 ** attempt)
                            continue
                        r.raise_for_status()
                        return await r.json()
                except (aiohttp.ClientError, asyncio.TimeoutError):
                    await asyncio.sleep(2 ** attempt)
        raise RuntimeError(f"request failed: {path} {params}")


async def get_symbols(c: Client):
    info = await c.get("/fapi/v1/exchangeInfo")
    return sorted(
        s["symbol"] for s in info["symbols"]
        if s.get("contractType") == "PERPETUAL"
        and s.get("quoteAsset") == "USDT"
        and s.get("status") == "TRADING"
    )


async def get_klines(c: Client, symbol, start_ms, end_ms, path="/fapi/v1/klines"):
    out, cur = [], start_ms
    while cur < end_ms:
        data = await c.get(path, {
            "symbol": symbol, "interval": "1h",
            "startTime": cur, "endTime": end_ms, "limit": 1500})
        if not data:
            break
        out.extend(data)
        nxt = data[-1][0] + HOUR_MS
        if nxt <= cur or len(data) < 1500:
            break
        cur = nxt
    return out


class Series:
    def __init__(self, kl):
        self.t = [k[0] for k in kl]
        self.o = [float(k[1]) for k in kl]
        self.h = [float(k[2]) for k in kl]
        self.l = [float(k[3]) for k in kl]
        self.c = [float(k[4]) for k in kl]
        qv = [float(k[7]) for k in kl]
        self.cum = [0.0]
        for v in qv:
            self.cum.append(self.cum[-1] + v)
        self.n = len(kl)

    def vol(self, a, b):
        """[a, b) 구간 거래대금 합."""
        a, b = max(a, 0), min(b, self.n)
        return self.cum[b] - self.cum[a] if b > a else 0.0

    def baseline24(self, s):
        """인덱스 s 직전 BASELINE 시간의 평균 24h 거래대금."""
        a = s - BASELINE
        if a < 0:
            return None
        return self.vol(a, s) / (BASELINE / 24)


def ratio(a, b):
    return a / b if a is not None and b else None


def scan(sr: Series, scan_from_ms, args):
    hits = []
    for i in range(WINDOW - 1, sr.n):
        s = i - WINDOW + 1
        if sr.t[i] < scan_from_ms or sr.o[s] <= 0:
            continue
        vol = sr.vol(s, i + 1)
        if vol < args.min_vol:
            continue
        base = sr.o[s]
        cg = sr.c[i] / base - 1
        hg = max(sr.h[s:i + 1]) / base - 1
        if cg < args.min_gain and hg < args.min_gain:
            continue
        vx = ratio(vol, sr.baseline24(s))
        if args.min_vol_ratio and (vx is None or vx < args.min_vol_ratio):
            continue
        hits.append({"s": s, "i": i, "vol": vol, "cg": cg, "hg": hg, "vx": vx})
    return hits


def merge(hits, sr: Series):
    events = []
    for h in hits:
        if events and sr.t[h["s"]] - (sr.t[events[-1]["i"]] + HOUR_MS) <= MERGE_GAP_MS:
            e = events[-1]
            e["i"] = h["i"]
            e["hits"] += 1
            for k in ("vol", "cg", "hg"):
                e[k] = max(e[k], h[k])
            if h["vx"] is not None:
                e["vx"] = max(e["vx"] or 0, h["vx"])
        else:
            events.append({**h, "hits": 1})
    return events


def enrich_price(e, sr: Series):
    s0, i1 = e["s"], e["i"]
    e["start_ms"] = sr.t[s0]
    e["end_ms"] = sr.t[i1] + HOUR_MS
    # Xu & Livshits: 선행 신호
    e["pre_ret"] = (sr.c[s0 - 1] / sr.o[s0 - PRE] - 1) if s0 - PRE >= 0 else None
    e["pre_vx"] = ratio(sr.vol(s0 - 24, s0), sr.baseline24(s0 - 24)) if s0 - 24 >= 0 else None
    # 피크 이후 덤프
    peak = max(range(s0, i1 + 1), key=sr.h.__getitem__)
    post = sr.l[peak + 1: peak + 1 + POST]
    e["peak_ms"] = sr.t[peak]
    e["dd"] = (min(post) / sr.h[peak] - 1) if post else None
    e["dd_partial"] = len(post) < POST


async def get_oi(c: Client, symbol, start_ms, end_ms, now_ms):
    start_ms = max(start_ms, now_ms - OI_LIMIT_MS + HOUR_MS)
    end_ms = min(end_ms, now_ms)
    if start_ms >= end_ms:
        return None, None
    rows, cur = [], start_ms
    while cur < end_ms:
        chunk_end = min(end_ms, cur + 499 * HOUR_MS)
        data = await c.get("/futures/data/openInterestHist", {
            "symbol": symbol, "period": "1h",
            "startTime": cur, "endTime": chunk_end, "limit": 500})
        if data:
            rows.extend(data)
        cur = chunk_end + HOUR_MS
    if not rows:
        return None, None
    rows.sort(key=lambda r: r["timestamp"])
    vals = [float(r["sumOpenInterestValue"]) for r in rows]
    peak_idx = max(range(len(vals)), key=vals.__getitem__)
    base = min(vals[:peak_idx + 1])
    growth = vals[peak_idx] / base - 1 if base > 0 else None
    return vals[peak_idx], growth


async def get_min_funding(c: Client, symbol, start_ms, end_ms):
    data = await c.get("/fapi/v1/fundingRate", {
        "symbol": symbol, "startTime": start_ms, "endTime": end_ms, "limit": 1000})
    if not data:
        return None
    return min(float(d["fundingRate"]) for d in data)


async def get_min_premium(c: Client, symbol, start_ms, end_ms):
    data = await get_klines(c, symbol, start_ms, end_ms, "/fapi/v1/premiumIndexKlines")
    if not data:
        return None
    return min(float(k[3]) for k in data)


def pass_deriv_filter(e, args):
    if args.max_prem is not None and (e["min_prem"] is None or e["min_prem"] > args.max_prem):
        return False
    if args.max_funding is not None and (e["min_funding"] is None or e["min_funding"] > args.max_funding):
        return False
    return True


async def process_symbol(c, symbol, scan_from, end_ms, now_ms, args):
    try:
        kl = await get_klines(c, symbol, scan_from - LOOKBACK * HOUR_MS, end_ms)
        kl = [k for k in kl if k[6] < now_ms]  # 마감된 봉만
        if len(kl) < WINDOW:
            return []
        sr = Series(kl)
        events = merge(scan(sr, scan_from + (WINDOW - 1) * HOUR_MS, args), sr)
        for e in events:
            enrich_price(e, sr)
            s, t = e["start_ms"], e["end_ms"]
            (e["peak_oi"], e["oi_growth"]), e["min_funding"], e["min_prem"] = \
                await asyncio.gather(
                    get_oi(c, symbol, s - WINDOW * HOUR_MS, t, now_ms),
                    get_min_funding(c, symbol, s, t),
                    get_min_premium(c, symbol, s, t))
            e["symbol"] = symbol.removesuffix("USDT")
        return [e for e in events if pass_deriv_filter(e, args)]
    except Exception as ex:  # noqa: BLE001
        print(f"[warn] {symbol}: {ex}")
        return []


# ---------------------------------------------------------------- 라이브 모드 (작업 B)
async def live_symbol(c: Client, symbol: str, end_ms: int, now_ms: int,
                      prem_map: dict, args):
    try:
        kl = await get_klines(c, symbol, end_ms - LOOKBACK_LIVE * HOUR_MS, end_ms)
        kl = [k for k in kl if k[6] < now_ms]  # 마감된 봉만
        if len(kl) < BASELINE + PRE:
            return None  # 상장 기간 부족

        sr = Series(kl)
        i = sr.n - 1
        s24 = i - 23
        vol24 = sr.vol(s24, i + 1)
        base24 = sr.baseline24(s24)
        volx = ratio(vol24, base24)
        ret24 = sr.c[i] / sr.o[s24] - 1
        ret72 = sr.c[i] / sr.o[i - 71] - 1

        prev_6h_avg = sr.vol(i - 29, i - 5) / 4.0
        vol_6h_x = ratio(sr.vol(i - 5, i + 1), prev_6h_avg)
        high72 = max(sr.h[i - 71:i + 1])
        from_high = sr.c[i] / high72 - 1 if high72 > 0 else 0.0

        # 1차 필터
        if vol24 < args.live_min_vol:
            return None
        if not ((volx is not None and volx >= args.live_min_volx) or ret72 >= args.live_min_ret72 or (vol_6h_x is not None and vol_6h_x >= 2.0)):
            return None

        # 이미 끝난 펌핑 제외 (덤프 진행 중)
        if from_high < -0.30:
            return None

        p_info = prem_map.get(symbol, {})
        prem_now = p_info.get("prem")
        fund_now = p_info.get("fund")
        price = p_info.get("mark", sr.c[i])

        return {
            "symbol": symbol,
            "sym_display": symbol.removesuffix("USDT"),
            "price": price,
            "vol24": vol24,
            "volx": volx,
            "vol_6h_x": vol_6h_x,
            "ret24": ret24,
            "ret72": ret72,
            "from_high": from_high,
            "prem_now": prem_now,
            "fund_now": fund_now,
            "last_t": sr.t[i],
        }
    except Exception as ex:  # noqa: BLE001
        print(f"[warn] {symbol}: {ex}")
        return None


async def fetch_oi_change(c: Client, symbol: str):
    try:
        data = await c.get("/futures/data/openInterestHist", {
            "symbol": symbol, "period": "1h", "limit": 25})
        if not data or len(data) < 2:
            return None, 0.0
        data.sort(key=lambda r: r["timestamp"])
        oi_now = float(data[-1]["sumOpenInterestValue"])
        oi_first = float(data[0]["sumOpenInterestValue"])
        oi_chg = (oi_now / oi_first - 1) if oi_first > 0 else None
        return oi_chg, oi_now
    except Exception:
        return None, 0.0


_MCAP_CACHE = {"data": {}, "last_update": 0}


async def get_market_caps(session: aiohttp.ClientSession) -> dict:
    """바이낸스 공식 마케팅 API에서 전체 심볼의 시가총액(Market Cap) 조회 (1시간 캐싱)."""
    now = time.time()
    if _MCAP_CACHE["data"] and (now - _MCAP_CACHE["last_update"]) < 3600:
        return _MCAP_CACHE["data"]
    try:
        url = "https://www.binance.com/bapi/composite/v1/public/marketing/symbol/list"
        async with session.get(url, headers=DEFAULT_HEADERS, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 200:
                res = await r.json()
                items = res.get("data", [])
                m_map = {}
                for it in items:
                    sym = it.get("symbol")
                    mcap = float(it.get("marketCap") or 0)
                    if sym and mcap > 0:
                        m_map[sym] = mcap
                if m_map:
                    _MCAP_CACHE["data"] = m_map
                    _MCAP_CACHE["last_update"] = now
                    return m_map
    except Exception as ex:
        print(f"[warn] fetch market cap failed: {ex}")
    return _MCAP_CACHE["data"]


async def fetch_recent_metrics(c: Client, symbol: str):
    """최근 15분 및 1시간 실시간 선물 거래대금, Taker 매수 비중(TIB), 롱숏 계정/포지션 비율 조회."""
    try:
        kl_task = c.get("/fapi/v1/klines", {
            "symbol": symbol, "interval": "15m", "limit": 5
        })
        pos_task = c.get("/futures/data/topLongShortPositionRatio", {
            "symbol": symbol, "period": "15m", "limit": 1
        })
        acc_task = c.get("/futures/data/globalLongShortAccountRatio", {
            "symbol": symbol, "period": "15m", "limit": 1
        })
        kl, pos, acc = await asyncio.gather(kl_task, pos_task, acc_task, return_exceptions=True)

        v15, v1h, taker_buy_pct, tib = 0.0, 0.0, 50.0, 0.0
        if isinstance(kl, list) and len(kl) >= 2:
            v15_closed = float(kl[-2][7])
            v15_cur = float(kl[-1][7])
            v15 = max(v15_closed, v15_cur)
            v1h = sum(float(k[7]) for k in kl[-5:-1])

            # 더 큰 거래량이 실린 15분 봉의 Taker 시장가 매수 비중 산출 (인덱스 10: Taker Buy Quote Vol)
            target_k = kl[-2] if v15 == v15_closed else kl[-1]
            tot_q = float(target_k[7])
            tk_buy_q = float(target_k[10])
            if tot_q > 0:
                ratio = min(max(tk_buy_q / tot_q, 0.0), 1.0)
                taker_buy_pct = ratio * 100.0
                tib = 2.0 * ratio - 1.0

        lsr_whale = None
        if isinstance(pos, list) and len(pos) > 0 and isinstance(pos[-1], dict) and "longShortRatio" in pos[-1]:
            try:
                lsr_whale = float(pos[-1]["longShortRatio"])
            except (ValueError, TypeError):
                pass

        lsr_retail = None
        if isinstance(acc, list) and len(acc) > 0 and isinstance(acc[-1], dict) and "longShortRatio" in acc[-1]:
            try:
                lsr_retail = float(acc[-1]["longShortRatio"])
            except (ValueError, TypeError):
                pass

        squeeze_div = None
        if lsr_whale is not None and lsr_retail is not None and lsr_retail > 0:
            squeeze_div = lsr_whale / max(lsr_retail, 0.05)

        return {
            "vol_15m": v15,
            "vol_1h": v1h,
            "taker_buy_pct": taker_buy_pct,
            "tib": tib,
            "lsr_whale": lsr_whale,
            "lsr_retail": lsr_retail,
            "squeeze_div": squeeze_div,
        }
    except Exception:
        return {
            "vol_15m": 0.0,
            "vol_1h": 0.0,
            "taker_buy_pct": 50.0,
            "tib": 0.0,
            "lsr_whale": None,
            "lsr_retail": None,
            "squeeze_div": None,
        }


async def scan_live_candidates(c: Client, args):
    now_ms = int(time.time() * 1000)
    end_ms = now_ms // HOUR_MS * HOUR_MS

    # 1. 24hr 티커, 프리미엄 인덱스, 시가총액 일괄 조회 (525개 klines 호출로 인한 418 IP 차단 원천 방지)
    tickers_task = c.get("/fapi/v1/ticker/24hr")
    prem_task = c.get("/fapi/v1/premiumIndex")
    mcap_task = get_market_caps(c.s)
    ticker_list, prem_list, mcap_map = await asyncio.gather(tickers_task, prem_task, mcap_task)

    # 24h 거래대금 하한 기준(args.live_min_vol)으로 대상 심볼 사전 필터링 (약 30~40개로 압축)
    vol_cutoff = args.live_min_vol * 0.90
    target_symbols = []
    for t in ticker_list:
        sym = t.get("symbol", "")
        if sym.endswith("USDT"):
            try:
                qv = float(t.get("quoteVolume", 0))
                if qv >= vol_cutoff:
                    target_symbols.append(sym)
            except (ValueError, TypeError):
                continue

    if not target_symbols:
        all_syms = await get_symbols(c)
        target_symbols = all_syms

    target_set = set(target_symbols)
    prem_map = {}
    for p in prem_list:
        sym = p.get("symbol")
        if sym in target_set:
            try:
                m_p = float(p.get("markPrice", 0))
                i_p = float(p.get("indexPrice", 0))
                f_r = float(p.get("lastFundingRate", 0))
                prem = (m_p / i_p - 1.0) if i_p > 0 else None
                prem_map[sym] = {"prem": prem, "fund": f_r, "mark": m_p}
            except (ValueError, TypeError):
                continue

    # 2. 선별된 대상 종목에 대해서만 정밀 klines 조회 (요청 수 525개 -> 30여개로 90% 이상 절감)
    candidates = await asyncio.gather(*(
        live_symbol(c, s, end_ms, now_ms, prem_map, args) for s in target_symbols))
    candidates = [cand for cand in candidates if cand is not None]

    # 1차 통과 종목 대상 OI 변화율 및 실시간 미시구조 지표(TIB, 롱숏비율) 조회
    if candidates:
        oi_tasks = [fetch_oi_change(c, cand["symbol"]) for cand in candidates]
        metrics_tasks = [fetch_recent_metrics(c, cand["symbol"]) for cand in candidates]
        oi_results, metrics_results = await asyncio.gather(
            asyncio.gather(*oi_tasks),
            asyncio.gather(*metrics_tasks)
        )
        for cand, oi_res, metrics in zip(candidates, oi_results, metrics_results):
            if isinstance(oi_res, tuple):
                cand["oi_chg24"] = oi_res[0]
                cand["oi_total"] = oi_res[1]
            else:
                cand["oi_chg24"] = oi_res
                cand["oi_total"] = 0.0
            cand["vol_15m"] = metrics["vol_15m"]
            cand["vol_1h"] = metrics["vol_1h"]
            cand["taker_buy_pct"] = metrics["taker_buy_pct"]
            cand["tib"] = metrics["tib"]
            cand["lsr_whale"] = metrics["lsr_whale"]
            cand["lsr_retail"] = metrics["lsr_retail"]
            cand["squeeze_div"] = metrics["squeeze_div"]

            sym = cand["symbol"]
            mcap = mcap_map.get(sym, 0.0)
            cand["mcap"] = mcap
            cand["vol_15m_mcap_pct"] = (metrics["vol_15m"] / mcap * 100.0) if mcap > 0 else 0.0
            cand["vol_1h_mcap_pct"] = (metrics["vol_1h"] / mcap * 100.0) if mcap > 0 else 0.0
            cand["vol24_mcap_pct"] = (cand["vol24"] / mcap * 100.0) if mcap > 0 else 0.0

            # 4대 팩터 통합 종합 점수식 (v4)
            vx = cand["volx"] or 0.0
            v6x = cand["vol_6h_x"] or 0.0
            v15_pct = cand["vol_15m_mcap_pct"]
            tib = cand["tib"]
            sq_div = cand["squeeze_div"]
            fund_now = cand["fund_now"] or 0.0
            prem_now = cand["prem_now"] or 0.0
            oi_val = cand["oi_chg24"] or 0.0
            ret24 = cand["ret24"] or 0.0

            # 1. 거래량 배수 (최대 8.0점)
            s_vol = min(vx, 30.0) / 10.0 + min(v6x, 10.0) / 2.0

            # 2. 시총 회전율 (최대 6.0점)
            s_mcap = min(v15_pct / 5.0, 6.0) if v15_pct >= 3.0 else 0.0

            # 3. 체결 공격성 TIB (최대 5.0점: 시장가 매수 75%면 +2.5점, 85%면 +3.5점)
            s_tib = max(0.0, tib) * 5.0

            # 4. 숏스퀴즈 다이버전스 & 음펀비/괴리 (최대 8.0점)
            s_div = max(0.0, sq_div - 1.0) * 3.0 if sq_div else 0.0
            s_fund = max(0.0, -fund_now) * 100.0
            s_prem = max(0.0, -prem_now) * 20.0
            s_squeeze = min(s_div + s_fund + s_prem, 8.0)

            # 5. 미결제약정 & 단기 모멘텀 (최대 4.0점)
            s_oi = max(0.0, oi_val) * 2.0
            s_ret = max(0.0, min(ret24, 0.50)) * 4.0

            cand["score"] = s_vol + s_mcap + s_tib + s_squeeze + s_oi + s_ret

        candidates.sort(key=lambda x: x["score"], reverse=True)
        top_candidates = candidates[:args.top]
    else:
        top_candidates = []

    last_closed_ms = end_ms - HOUR_MS
    return top_candidates, last_closed_ms


async def run_live(c: Client, args):
    t0 = time.time()
    cond = f"Vol24>={fmt_usd(args.live_min_vol)}, (VolX>={args.live_min_volx:g}x OR Ret72>={args.live_min_ret72:.0%})"
    print(f"Scanning perpetuals in LIVE mode ({cond}) ...")

    top_candidates, last_closed_ms = await scan_live_candidates(c, args)
    as_of_str = datetime.fromtimestamp(last_closed_ms / 1000, tz=timezone.utc).strftime("%m-%d %H:%M")
    print(f"\nAs of {as_of_str} UTC (last closed 1h bar) - Scan time: {time.time() - t0:.1f}s")

    headers = ["#", "Symbol", "Price", "Vol24h", "VolX", "Vol6hX",
               "Ret24h", "Ret72h", "FromHigh", "OI24h%", "Fund", "Prem", "Score"]
    rows = []
    for idx, d in enumerate(top_candidates, 1):
        rows.append([
            str(idx),
            d["sym_display"],
            f"{d['price']:.5g}",
            fmt_usd(d["vol24"]),
            fmt_x(d["volx"]),
            fmt_x(d["vol_6h_x"]),
            fmt_pct(d["ret24"]),
            fmt_pct(d["ret72"]),
            fmt_pct(d["from_high"]),
            fmt_pct(d["oi_chg24"]),
            fmt_pct(d["fund_now"], 3),
            fmt_pct(d["prem_now"], 2),
            f"{d['score']:.1f}",
        ])

    print_table("조기경보 (live)", headers, rows)
    print("""
  VolX     24h 거래대금 / 직전 7일 평균 24h       [Kamps&Kleinberg]
  Vol6hX   최근 6h 거래대금 / 직전 24h의 6h 평균 (가속도)
  Ret72h   최근 72h 수익률                       FromHigh 72h 고점 대비 하락폭 (>-30% 필터)
  OI24h%   최근 24h 미결제약정(OI) 변화율          Prem     현재 프리미엄 지수 (음수=선물 디스카운트)
  Score    조기경보 종합 점수 (거래량 폭증 + 추세 + 음의 괴리율 + OI 증가 가중합)""")


# ---------------------------------------------------------------- 출력
def fmt_ts(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%m-%d %H:%M")


def fmt_usd(v):
    if v is None:
        return "-"
    for unit, div in (("B", 1e9), ("M", 1e6), ("K", 1e3)):
        if abs(v) >= div:
            return f"${v / div:.2f}{unit}" if unit == "B" else f"${v / div:.1f}{unit}"
    return f"${v:.0f}"


def fmt_pct(v, digits=1):
    return "-" if v is None else f"{v * 100:+.{digits}f}%"


def fmt_x(v):
    return "-" if v is None else f"{v:.1f}x"


def print_table(title, headers, rows):
    print(f"\n■ {title} ({len(rows)})")
    if not rows:
        print("  (없음)")
        return
    widths = [max(len(h), *(len(r[j]) for r in rows)) for j, h in enumerate(headers)]
    line = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    left = {1, 2}  # 종목, 기간(또는 가격)은 좌측 정렬
    fmt = lambda r: "| " + " | ".join(
        c.ljust(w) if j in left else c.rjust(w)
        for j, (c, w) in enumerate(zip(r, widths))) + " |"
    print(line); print(fmt(headers)); print(line)
    for r in rows:
        print(fmt(r))
    print(line)


def row(idx, e, with_oi):
    period = f"{fmt_ts(e['start_ms'])} → {fmt_ts(e['end_ms'])}"
    r = [str(idx), e["symbol"], period, str(e["hits"]),
         fmt_usd(e["vol"]), fmt_x(e["vx"]),
         fmt_pct(e["cg"]), fmt_pct(e["hg"]),
         fmt_pct(e["pre_ret"]), fmt_x(e["pre_vx"]),
         fmt_pct(e["dd"]) + ("*" if e["dd_partial"] and e["dd"] is not None else "")]
    if with_oi:
        r += [fmt_usd(e["peak_oi"]), fmt_pct(e["oi_growth"], 0)]
    r += [fmt_pct(e["min_funding"], 3), fmt_pct(e["min_prem"], 2)]
    return r


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--min-vol", type=float, default=1e9, help="24h 거래대금 하한 (USD)")
    ap.add_argument("--min-gain", type=float, default=0.60, help="종가/고가 상승률 하한")
    ap.add_argument("--min-vol-ratio", type=float, default=0.0,
                    help="K&K식 상대 거래량 필터: 24h 거래대금 / 직전 7일 평균 (0=끔)")
    ap.add_argument("--max-prem", type=float, default=None,
                    help="최저 premium index 상한 (예: -0.10 = -10%%)")
    ap.add_argument("--max-funding", type=float, default=None,
                    help="최저 펀딩비 상한 (예: -0.005 = -0.5%%)")
    ap.add_argument("--live", action="store_true",
                    help="조기경보 라이브 모드 실행")
    ap.add_argument("--live-min-vol", type=float, default=3e7,
                    help="라이브: 24h 거래대금 하한 (기본 3천만달러)")
    ap.add_argument("--live-min-volx", type=float, default=2.0,
                    help="라이브: VolX 하한 (기본 2.0x)")
    ap.add_argument("--live-min-ret72", type=float, default=0.10,
                    help="라이브: 최근 72h 수익률 하한 (기본 +10%%)")
    ap.add_argument("--top", type=int, default=30,
                    help="라이브: 출력 최대 행 수 (기본 30)")
    ap.add_argument("--concurrency", type=int, default=CONCURRENCY)
    args = ap.parse_args()

    now_ms = int(time.time() * 1000)
    end_ms = now_ms // HOUR_MS * HOUR_MS
    scan_from = end_ms - args.days * 24 * HOUR_MS - (WINDOW - 1) * HOUR_MS

    t0 = time.time()
    async with aiohttp.ClientSession() as session:
        c = Client(session, args.concurrency)

        # 라이브 모드 분기
        if args.live:
            await run_live(c, args)
            return

        symbols = await get_symbols(c)
        cond = f"vol>={fmt_usd(args.min_vol)}, gain>={args.min_gain:.0%}"
        if args.min_vol_ratio:
            cond += f", VolX>={args.min_vol_ratio:g}x"
        if args.max_prem is not None:
            cond += f", MinPrem<={args.max_prem * 100:.1f}%"
        if args.max_funding is not None:
            cond += f", MinFund<={args.max_funding * 100:.3f}%"
        print(f"Scanning {len(symbols)} USDT-M perpetuals, last {args.days}d ({cond}) ...")
        results = await asyncio.gather(*(
            process_symbol(c, s, scan_from, end_ms, now_ms, args) for s in symbols))

    events = sorted((e for r in results for e in r), key=lambda e: e["start_ms"])
    print(f"\n{len(events)} events found ({time.time() - t0:.1f}s)")

    oi_cut = now_ms - OI_LIMIT_MS
    recent = [e for e in events if e["end_ms"] > oi_cut]
    old = [e for e in events if e["end_ms"] <= oi_cut]
    base_h = ["#", "Symbol", "Period (UTC)", "Hrs", "Vol24h", "VolX",
              "Close%", "High%", "Pre72h", "PreVolX", "DD72h"]
    tail_h = ["MinFund", "MinPrem"]
    print_table("최근 30일 (OI 포함)", base_h + ["PeakOI", "OI%"] + tail_h,
                [row(i, e, True) for i, e in enumerate(recent, 1)])
    print_table("30일 이전 (OI 없음)", base_h + tail_h,
                [row(i, e, False) for i, e in enumerate(old, len(recent) + 1)])

    print("""
  Hrs     조건 충족 시간봉 수                  VolX    24h 거래대금 / 직전 7일 평균  [Kamps&Kleinberg]
  Pre72h  이벤트 직전 72h 수익률               PreVolX 직전 24h 거래대금 / 그 이전 7일 평균  [Xu&Livshits]
  DD72h   피크 후 72h 최대 낙폭 (*=72h 미경과)  OI%     시작 24h 전~피크 최저 OI 대비 피크 OI
  MinFund 이벤트 중 최저 펀딩비                MinPrem 최저 premium index (음수=선물 디스카운트) [He et al.]""")


if __name__ == "__main__":
    asyncio.run(main())
