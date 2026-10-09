#!/usr/bin/env bash
# 바이낸스 급등 조기경보 봇 로컬 백그라운드 시작 스크립트 (caffeinate 잠자기 방지 내장)

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

# 기존 실행 중인 봇 및 관련 caffeinate 프로세스 종료
pkill -f "python.*bot.py" 2>/dev/null
pkill -f "caffeinate.*bot.py" 2>/dev/null

echo "=========================================="
echo "🚀 Binance Alert Bot 로컬 백그라운드 가동"
echo "☕ caffeinate 잠자기 방지 모드 적용 (화면 꺼져도 작동)"
echo "=========================================="

PYTHON_BIN="/Users/younguk/antigravity/.venv/bin/python"
if [ ! -f "$PYTHON_BIN" ]; then
    PYTHON_BIN="python3"
fi

# caffeinate -s : 봇 프로세스가 살아있는 동안 시스템 잠자기(절전모드)를 자동 방지
nohup caffeinate -s "$PYTHON_BIN" bot.py > bot_local.log 2>&1 &
PID=$!

echo "✅ 봇이 백그라운드에서 시작되었습니다! (PID: $PID)"
echo "• 로그 실시간 확인: tail -f $DIR/bot_local.log"
echo "• 봇 종료 명령어:   ./stop_local.sh"
echo "• 상태 확인 명령어: ./status_local.sh"
echo "=========================================="

# macOS 알림 배너 띄우기
osascript -e 'display notification "15분마다 바이낸스 선물을 스캔합니다 (잠자기 방지 On)" with title "Binance Alert Bot" subtitle "백그라운드 가동 시작 🟢"' 2>/dev/null || true
