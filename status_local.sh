#!/usr/bin/env bash
# 바이낸스 급등 조기경보 봇 상태 확인 스크립트

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

echo "=========================================="
echo "📊 Binance Alert Bot 실행 상태"
echo "=========================================="

PID=$(pgrep -f "python.*bot.py")
if [ -n "$PID" ]; then
    echo "🟢 상태: 정상 가동 중 (PID: $PID)"
    echo "• 메모리/CPU: $(ps -o %cpu,%mem,etime -p $PID | tail -n 1)"
else
    echo "🔴 상태: 정지됨 (실행 중이지 않음)"
fi

echo "------------------------------------------"
echo "📜 최근 로그 (마지막 15줄):"
if [ -f "bot_local.log" ]; then
    tail -n 15 bot_local.log
else
    echo "로그 파일 없음"
fi
echo "=========================================="
