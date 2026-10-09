#!/usr/bin/env bash
# 바이낸스 급등 조기경보 봇 종료 스크립트

echo "🛑 Binance Alert Bot 종료 중..."
pkill -f "python.*bot.py" 2>/dev/null
pkill -f "caffeinate.*bot.py" 2>/dev/null

if [ $? -eq 0 ]; then
    echo "✅ 봇이 정상적으로 종료되었습니다. (잠자기 방지도 해제됨)"
    osascript -e 'display notification "스캐너 봇이 종료되었습니다." with title "Binance Alert Bot" subtitle "가동 정지 🔴"' 2>/dev/null || true
else
    echo "ℹ️ 실행 중인 봇이 없습니다."
fi
