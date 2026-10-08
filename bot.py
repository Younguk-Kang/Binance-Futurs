#!/usr/bin/env python3
"""Binance Early Warning Alert Bot for Render.com

- 15분(또는 설정된 주기)마다 바이낸스 USDT-M 무기한 선물 전체를 스캔
- Kamps & Kleinberg (2018), Xu & Livshits (2019), He et al. (2022) 기반 종합 점수 산출
- 상위 감지 종목을 텔레그램 봇으로 즉시 알림
- Render.com 무료 Web Service 호환용 Health Check 웹서버 내장
"""
import argparse
import asyncio
import os
import time
from datetime import datetime, timezone

import aiohttp
from aiohttp import web

from scanner import Client, CONCURRENCY, fmt_usd, fmt_pct, fmt_x, scan_live_candidates

# 환경변수 설정
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
SCAN_INTERVAL_MIN = int(os.getenv("SCAN_INTERVAL_MIN", "15"))
MIN_SCORE_NOTIFY = float(os.getenv("MIN_SCORE_NOTIFY", "3.0"))
COOLDOWN_HOURS = float(os.getenv("COOLDOWN_HOURS", "4.0"))
PORT = int(os.getenv("PORT", "10000"))

# 상태 관리 (중복 알림 방지용 캐시)
# symbol -> {"last_time": timestamp, "score": float, "price": float}
alert_cache = {}
bot_state = {
    "start_time": time.time(),
    "last_scan_time": None,
    "last_scan_count": 0,
    "total_alerts_sent": 0,
}


async def send_telegram(session: aiohttp.ClientSession, text: str) -> bool:
    """텔레그램 메시지 발송."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[telegram] 토큰 또는 CHAT_ID가 설정되지 않아 콘솔 출력으로 대체합니다.")
        print(text)
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
                print(f"[telegram error] status={r.status}: {res}")
        except Exception as ex:
            print(f"[telegram fail] attempt {attempt+1}: {ex}")
        await asyncio.sleep(2 ** attempt)
    return False


def should_alert(item: dict) -> bool:
    """중복 알림 방지(쿨다운) 및 급등 예외 검사."""
    sym = item["symbol"]
    score = item["score"]
    now = time.time()

    if score < MIN_SCORE_NOTIFY:
        return False

    if sym not in alert_cache:
        alert_cache[sym] = {"last_time": now, "score": score, "price": item["price"]}
        return True

    prev = alert_cache[sym]
    elapsed_hours = (now - prev["last_time"]) / 3600.0

    # 1. 쿨다운 시간(기본 4시간) 경과 시 재알림 허용
    if elapsed_hours >= COOLDOWN_HOURS:
        alert_cache[sym] = {"last_time": now, "score": score, "price": item["price"]}
        return True

    # 2. 쿨다운 중이라도 점수가 35% 이상 급상승한 경우 예외 알림
    if score >= prev["score"] * 1.35 and score >= 6.0:
        alert_cache[sym] = {"last_time": now, "score": score, "price": item["price"]}
        return True

    # 3. 펀딩비가 -1.5% 이하로 극단적 음수 폭락한 경우 예외 알림
    fund = item.get("fund_now") or 0.0
    if fund <= -0.015 and prev.get("fund_notified", 0) != fund:
        prev["fund_notified"] = fund
        prev["last_time"] = now
        return True

    return False


def format_alert_message(d: dict, as_of_str: str) -> str:
    """가독성 높은 텔레그램 HTML 카드 메시지 생성."""
    sym = d["sym_display"]
    score = d["score"]
    vx = d["volx"] or 0.0
    v6x = d["vol_6h_x"] or 0.0
    oi_chg = d.get("oi_chg24")
    prem = d.get("prem_now")
    fund = d.get("fund_now")

    score_badge = "🔥 [초강력 펌핑 신호]" if score >= 10.0 else "⚡ [조기경보 감지]"

    msg = (
        f"🚨 <b>{score_badge}</b> <code>#{sym}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>종합 랭킹 점수:</b> <b>{score:.1f}점</b>\n"
        f"💵 <b>현재 가격:</b> <code>${d['price']:.5g}</code>\n"
        f"💰 <b>24h 거래대금:</b> {fmt_usd(d['vol24'])} (평소의 <b>{vx:.1f}배</b>)\n"
        f"⚡ <b>6h 가속도:</b> {v6x:.1f}x\n"
        f"🚀 <b>수익률:</b> 24h <b>{fmt_pct(d['ret24'])}</b> | 72h {fmt_pct(d['ret72'])}\n"
        f"🔻 <b>72h 고점대비:</b> {fmt_pct(d['from_high'])}\n"
        f"📈 <b>24h OI 변화율:</b> <b>{fmt_pct(oi_chg, 1)}</b>\n"
        f"📉 <b>최신 펀딩비:</b> <code>{fmt_pct(fund, 3)}</code>\n"
        f"⚖️ <b>선물 괴리율(Prem):</b> {fmt_pct(prem, 2)}\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ <i>{as_of_str} UTC 봉 마감 기준</i>"
    )
    return msg


async def run_single_scan(session: aiohttp.ClientSession, args):
    """1회 실시간 스캔 및 알림 발송."""
    c = Client(session, args.concurrency)
    t0 = time.time()
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{now_str}] 실시간 스캔 시작 ...")

    try:
        candidates, last_closed_ms = await scan_live_candidates(c, args)
        as_of_str = datetime.fromtimestamp(last_closed_ms / 1000, tz=timezone.utc).strftime("%m-%d %H:%M")
        scan_sec = time.time() - t0
        print(f"[{now_str}] 스캔 완료 ({scan_sec:.1f}초, 후보 {len(candidates)}개 감지)")

        bot_state["last_scan_time"] = time.time()
        bot_state["last_scan_count"] = len(candidates)

        alerts_to_send = [c for c in candidates if should_alert(c)]

        if alerts_to_send:
            print(f"[{now_str}] 신규 알림 대상 {len(alerts_to_send)}개 발송 시작 ...")
            for item in alerts_to_send:
                text = format_alert_message(item, as_of_str)
                success = await send_telegram(session, text)
                if success:
                    bot_state["total_alerts_sent"] += 1
                await asyncio.sleep(0.5)  # 텔레그램 초당 발송 제한 방지
        else:
            print(f"[{now_str}] 새로운 알림 대상 없음 (모두 쿨다운 또는 점수 미달)")

    except Exception as ex:
        print(f"[{now_str}] [오류 발생] 스캔 루프 예외: {ex}")


async def scheduler_loop(args):
    """설정된 주기마다 무한 반복하는 스케줄러."""
    async with aiohttp.ClientSession() as session:
        # 기동 안내 메시지 (토큰 등록 시 1회 발송)
        startup_msg = (
            f"🤖 <b>[Binance Alert Bot 가동]</b>\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"• 스캔 주기: <b>{SCAN_INTERVAL_MIN}분</b>\n"
            f"• 최소 알림 점수: <b>{MIN_SCORE_NOTIFY}점</b>\n"
            f"• 쿨다운 시간: <b>{COOLDOWN_HOURS}시간</b>\n"
            f"• 상태: <b>정상 감시 중 🟢</b>"
        )
        await send_telegram(session, startup_msg)

        while True:
            await run_single_scan(session, args)
            sleep_sec = max(60, SCAN_INTERVAL_MIN * 60)
            print(f"[scheduler] 다음 스캔까지 {sleep_sec // 60}분간 대기합니다 ...\n")
            await asyncio.sleep(sleep_sec)


# ---------------------------------------------------------------- Render 웹서버 (Healthcheck)
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
        "last_scan_time": last_scan,
        "last_scan_count": bot_state["last_scan_count"],
        "total_alerts_sent": bot_state["total_alerts_sent"],
        "active_cache_symbols": list(alert_cache.keys()),
    }
    return web.json_response(data)


async def handle_health(request):
    return web.Response(text="OK", status=200)


def create_web_app():
    app = web.Application()
    app.router.add_get("/", handle_index)
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
    app = create_web_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print(f"🚀 Render Healthcheck 웹서버 시작: port {PORT}")

    # 스케줄러 실행
    await scheduler_loop(args)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("\n봇이 안전하게 종료되었습니다.")
