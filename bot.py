#!/usr/bin/env python3
"""Binance Early Warning Alert Bot for Render.com

- 15분(또는 설정된 주기)마다 바이낸스 USDT-M 무기한 선물 전체를 스캔
- Kamps & Kleinberg (2018), Xu & Livshits (2019), He et al. (2022) 기반 종합 점수 산출
- 상위 감지 종목을 텔레그램 봇으로 즉시 알림
- Render.com 무료 Web Service 호환용 Health Check 웹서버 & 실시간 디버그 엔드포인트 내장
"""
import argparse
import asyncio
import os
import sys
import time
import traceback
from datetime import datetime, timezone

import aiohttp
from aiohttp import web

from scanner import Client, CONCURRENCY, fmt_usd, fmt_pct, fmt_x, scan_live_candidates

# .env 파일 자동 로드 (로컬 실행 편의용)
env_path = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(env_path):
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

# 환경변수 설정
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
SCAN_INTERVAL_MIN = int(os.getenv("SCAN_INTERVAL_MIN", "15"))
MIN_SCORE_NOTIFY = float(os.getenv("MIN_SCORE_NOTIFY", "3.0"))
COOLDOWN_HOURS = float(os.getenv("COOLDOWN_HOURS", "24.0"))
PORT = int(os.getenv("PORT", "10000"))

# 상태 관리 (중복 알림 방지용 캐시 및 실시간 상태)
alert_cache = {}
bot_state = {
    "start_time": time.time(),
    "last_scan_time": None,
    "last_scan_count": 0,
    "total_alerts_sent": 0,
    "stage": "initializing",
    "scan_in_progress": False,
    "last_error": None,
    "last_error_time": None,
    "last_detected": [],
}


def log(msg: str):
    """실시간 stdout 출력 (버퍼링 방지)."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{now}] {msg}", flush=True)


async def send_telegram(session: aiohttp.ClientSession, text: str) -> bool:
    """텔레그램 메시지 발송."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("[telegram] 토큰 또는 CHAT_ID가 설정되지 않았습니다.")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    for attempt in range(3):
        try:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status == 200:
                    return True
                res = await r.text()
                log(f"[telegram error] status={r.status}: {res}")
        except Exception as ex:
            log(f"[telegram fail] attempt {attempt+1}: {ex}")
        await asyncio.sleep(2 ** attempt)
    return False


def should_alert(item: dict) -> bool:
    """중복 알림 방지(24시간 쿨다운). 한 번 포착된 종목은 24시간 거래량 초기화 후 다시 판단."""
    sym = item["symbol"]
    score = item["score"]
    v15_pct = item.get("vol_15m_mcap_pct") or 0.0
    now = time.time()

    # 종합 점수 기준 미달이면서 시총 대비 15분 거래량도 5% 미만이면 탈락
    if score < MIN_SCORE_NOTIFY and v15_pct < 5.0:
        return False

    if sym not in alert_cache:
        return True

    prev = alert_cache[sym]
    elapsed_hours = (now - prev["last_time"]) / 3600.0

    # 포착 후 24시간이 경과해야만 다시 판단 (24h 롤링 거래량 초기화 대기)
    if elapsed_hours >= COOLDOWN_HOURS:
        return True

    return False


def record_alert_sent(item: dict):
    """텔레그램 발송 성공 시에만 캐시 갱신 (24시간 카운트다운 시작)."""
    sym = item["symbol"]
    alert_cache[sym] = {
        "last_time": time.time(),
        "score": item["score"],
        "price": item["price"],
    }


def format_alert_message(d: dict, as_of_str: str) -> str:
    """이모티콘 제거, 티커 최상단 강조, 시총 대비 거래량 지표 적용 메시지 생성."""
    sym = d["sym_display"]
    score = d["score"]
    vx = d["volx"] or 0.0
    v6x = d["vol_6h_x"] or 0.0
    oi_chg = d.get("oi_chg24")
    prem = d.get("prem_now")
    fund = d.get("fund_now")

    mcap = d.get("mcap", 0.0)
    v15 = d.get("vol_15m", 0.0)
    v15_pct = d.get("vol_15m_mcap_pct", 0.0)
    v1h = d.get("vol_1h", 0.0)
    v1h_pct = d.get("vol_1h_mcap_pct", 0.0)
    v24_pct = d.get("vol24_mcap_pct", 0.0)

    # "조기경보" 대체: 트레이딩 관점의 직관적인 수급/모멘텀 용어
    if v15_pct >= 15.0 or score >= 10.0:
        badge = "[초강력 수급 폭증]"
    elif v15_pct >= 5.0 or score >= 3.0:
        badge = "[급등 시그널]"
    else:
        badge = "[수급 유입 감지]"

    # 시총 대비 수치 서식화
    mcap_str = fmt_usd(mcap) if mcap > 0 else "미확인"
    v15_pct_str = f"시총의 <b>{v15_pct:.1f}%</b>" if mcap > 0 else "-"
    v1h_pct_str = f"시총의 {v1h_pct:.1f}%" if mcap > 0 else "-"
    v24_pct_str = f"시총의 {v24_pct:.1f}% 회전" if mcap > 0 else "-"

    msg = (
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>{badge} {sym}USDT</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"종합 점수 : <b>{score:.1f}점</b>\n"
        f"현재 가격 : <b>${d['price']:.5g}</b>\n"
        f"\n"
        f"[가격 및 상승률]\n"
        f"• 24h 상승률 : <b>{fmt_pct(d['ret24'])}</b>\n"
        f"• 72h 상승률 : {fmt_pct(d['ret72'])}\n"
        f"• 72h 고점 대비 : {fmt_pct(d['from_high'])}\n"
        f"\n"
        f"[거래대금 및 수급 (시총 대비)]\n"
        f"• 갑작스런 거래량 발생 : <b>{fmt_usd(v15)}</b> (15분, {v15_pct_str})\n"
        f"• 1h 누적 거래량 : {fmt_usd(v1h)} ({v1h_pct_str})\n"
        f"• 24h 총 거래대금 : {fmt_usd(d['vol24'])} ({v24_pct_str})\n"
        f"• 유통 시가총액 : <b>{mcap_str}</b>\n"
        f"• 평균 대비 거래량 : <b>{vx:.1f}배</b> (6h 가속도 {v6x:.1f}배)\n"
        f"• 24h 미결제약정(OI) : <b>{fmt_pct(oi_chg, 1)}</b>\n"
        f"\n"
        f"[선물 파생 지표]\n"
        f"• 최신 펀딩비 : <b>{fmt_pct(fund, 3)}</b>\n"
        f"• 선물 괴리율 : {fmt_pct(prem, 2)}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{as_of_str} UTC 봉 마감 기준"
    )
    return msg


async def run_single_scan(session: aiohttp.ClientSession, args):
    """1회 실시간 스캔 및 알림 발송."""
    if bot_state["scan_in_progress"]:
        log("이미 다른 스캔이 진행 중입니다. 건너뜁니다.")
        return

    bot_state["scan_in_progress"] = True
    bot_state["stage"] = "scanning"
    c = Client(session, args.concurrency)
    t0 = time.time()
    log("실시간 스캔 시작 ...")

    try:
        candidates, last_closed_ms = await scan_live_candidates(c, args)
        as_of_str = datetime.fromtimestamp(last_closed_ms / 1000, tz=timezone.utc).strftime("%m-%d %H:%M")
        scan_sec = time.time() - t0
        log(f"스캔 완료 ({scan_sec:.1f}초, 후보 {len(candidates)}개 감지)")

        bot_state["last_scan_time"] = time.time()
        bot_state["last_scan_count"] = len(candidates)
        bot_state["last_detected"] = [
            {"symbol": cand["symbol"], "score": round(cand["score"], 1), "price": cand["price"]}
            for cand in candidates[:5]
        ]
        bot_state["stage"] = "evaluating_alerts"

        alerts_to_send = [c for c in candidates if should_alert(c)]

        if alerts_to_send:
            log(f"신규 알림 대상 {len(alerts_to_send)}개 발송 시작 ...")
            for item in alerts_to_send:
                text = format_alert_message(item, as_of_str)
                success = await send_telegram(session, text)
                if success:
                    record_alert_sent(item)
                    bot_state["total_alerts_sent"] += 1
                await asyncio.sleep(0.5)  # 텔레그램 초당 발송 제한 방지
        else:
            log("새로운 알림 대상 없음 (모두 쿨다운 또는 점수 미달)")

        bot_state["stage"] = "idle"

    except Exception as ex:
        err_msg = f"{type(ex).__name__}: {ex}"
        bot_state["last_error"] = err_msg
        bot_state["last_error_time"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        bot_state["stage"] = "error"
        log(f"[오류 발생] 스캔 루프 예외: {err_msg}")
        traceback.print_exc()

    finally:
        bot_state["scan_in_progress"] = False


async def scheduler_loop(args):
    """설정된 주기마다 무한 반복하는 스케줄러."""
    async with aiohttp.ClientSession() as session:
        # 기동 안내 메시지 (토큰 등록 시 1회 발송)
        startup_msg = (
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>[시스템 가동] 바이낸스 선물 급등 스캐너</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"• 스캔 주기 : {SCAN_INTERVAL_MIN}분\n"
            f"• 최소 점수 : {MIN_SCORE_NOTIFY}점\n"
            f"• 알림 쿨다운 : {COOLDOWN_HOURS:.0f}시간 (거래량 초기화 대기)\n"
            f"• 감시 상태 : 정상 가동 중"
        )
        await send_telegram(session, startup_msg)

        while True:
            await run_single_scan(session, args)
            sleep_sec = max(60, SCAN_INTERVAL_MIN * 60)
            log(f"[scheduler] 다음 스캔까지 {sleep_sec // 60}분간 대기합니다 ...\n")
            await asyncio.sleep(sleep_sec)


# ---------------------------------------------------------------- Render 웹서버 & 디버그 라우트
async def handle_index(request):
    uptime = int(time.time() - bot_state["start_time"])
    last_scan = (
        datetime.fromtimestamp(bot_state["last_scan_time"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        if bot_state["last_scan_time"]
        else "None"
    )
    data = {
        "status": "ok",
        "service": "binance-early-warning-bot",
        "uptime_seconds": uptime,
        "stage": bot_state["stage"],
        "scan_in_progress": bot_state["scan_in_progress"],
        "last_scan_time": last_scan,
        "last_scan_count": bot_state["last_scan_count"],
        "total_alerts_sent": bot_state["total_alerts_sent"],
        "last_detected": bot_state["last_detected"],
        "last_error": bot_state["last_error"],
        "last_error_time": bot_state["last_error_time"],
        "active_cache_symbols": list(alert_cache.keys()),
    }
    return web.json_response(data)


async def handle_debug(request):
    """Render 서버의 외부 IP 및 바이낸스 통신 상태 즉시 진단."""
    diag = {}
    async with aiohttp.ClientSession() as s:
        # 1. 서버 외부 IP 확인
        try:
            async with s.get("https://api.ipify.org?format=json", timeout=aiohttp.ClientTimeout(total=5)) as r:
                diag["server_ip"] = (await r.json()).get("ip")
        except Exception as e:
            diag["server_ip_error"] = str(e)

        # 2. 바이낸스 핑 테스트
        try:
            async with s.get("https://fapi.binance.com/fapi/v1/ping", timeout=aiohttp.ClientTimeout(total=5)) as r:
                diag["binance_ping_status"] = r.status
                diag["binance_weight_1m"] = r.headers.get("x-mbx-used-weight-1m")
        except Exception as e:
            diag["binance_ping_error"] = str(e)

        # 3. 바이낸스 1h 캔들 테스트 (BTCUSDT)
        try:
            async with s.get("https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=1h&limit=5",
                             timeout=aiohttp.ClientTimeout(total=5)) as r:
                diag["binance_klines_status"] = r.status
                if r.status != 200:
                    diag["binance_klines_response"] = await r.text()
        except Exception as e:
            diag["binance_klines_error"] = str(e)

    diag["bot_state"] = bot_state
    return web.json_response(diag)


async def handle_manual_scan(request):
    """웹 브라우저 접속으로 즉시 수동 1회 스캔 트리거."""
    args = request.app["cli_args"]
    asyncio.create_task(run_single_scan_bg(args))
    return web.json_response({"message": "수동 스캔이 시작되었습니다. 1분 후 메인 화면(/) 또는 텔레그램을 확인하세요."})


async def run_single_scan_bg(args):
    async with aiohttp.ClientSession() as session:
        await run_single_scan(session, args)


async def handle_health(request):
    return web.Response(text="OK", status=200)


def create_web_app(args):
    app = web.Application()
    app["cli_args"] = args
    app.router.add_get("/", handle_index)
    app.router.add_get("/debug", handle_debug)
    app.router.add_get("/scan", handle_manual_scan)
    app.router.add_get("/healthz", handle_health)
    return app


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--live-min-vol", type=float, default=2e8)
    parser.add_argument("--live-min-volx", type=float, default=3.0)
    parser.add_argument("--live-min-ret72", type=float, default=0.20)
    parser.add_argument("--top", type=int, default=30)
    parser.add_argument("--concurrency", type=int, default=CONCURRENCY)
    args = parser.parse_args()

    # 웹 서버 백그라운드 구동 (Render 포트 바인딩)
    app = create_web_app(args)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log(f"🚀 Render Healthcheck 웹서버 시작: port {PORT}")

    # 스케줄러 실행
    await scheduler_loop(args)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log("봇이 안전하게 종료되었습니다.")
