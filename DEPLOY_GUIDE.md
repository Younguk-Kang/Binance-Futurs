# 바이낸스 급등 조기경보 봇 — Render.com 배포 가이드

이 문서는 `deploy_render/` 디렉터리의 코드를 GitHub에 올리고, **Render.com(싱가포르 리전)**에 24시간 무중단 텔레그램 알림 봇으로 배포하는 전체 절차입니다.

---

## 1단계: 텔레그램 봇 토큰 및 Chat ID 준비 (2분)

### 1) 봇 생성 및 토큰 발급
1. 텔레그램 앱에서 **`@BotFather`** 검색 후 대화 시작
2. `/newbot` 입력
3. 봇의 이름(예: `My Pump Alert`) 입력
4. 봇의 유저네임(예: `my_binance_pump_bot`, 반드시 `bot`으로 끝나야 함) 입력
5. 완료되면 발급되는 **`HTTP API Token`** 복사  
   *(예: `7123456789:AAFlk...`)*

### 2) 내 Chat ID 확인
1. 내가 만든 봇을 검색해서 들어가 **`시작(Start)`** 버튼을 누릅니다.
2. 텔레그램 검색창에 **`@userinfobot`** 검색 후 대화 시작
3. 봇이 알려주는 내 **`Id` (숫자)** 복사  
   *(예: `123456789`)*

---

## 2단계: 로컬에서 텔레그램 발송 테스트 (선택)

배포하기 전에 내 컴퓨터에서 텔레그램 메시지가 잘 오는지 1회 테스트해볼 수 있습니다.

```bash
cd ~/antigravity/deploy_render

# 환경변수 설정 후 1회 테스트 실행
TELEGRAM_BOT_TOKEN="발급받은토큰" \
TELEGRAM_CHAT_ID="내아이디숫자" \
~/antigravity/.venv/bin/python bot.py
```
> 실행 시 텔레그램으로 `🤖 [Binance Alert Bot 가동]` 메시지가 도착하면 정상입니다! (확인 후 `Ctrl + C`로 종료)

---

## 3단계: GitHub에 코드 올리기

Render는 GitHub 저장소의 코드를 가져와서 자동으로 서버에 배포합니다.

### 1) GitHub에서 새 저장소 생성
1. [GitHub](https://github.com) 로그인 후 우측 상단 `+` → **New repository**
2. Repository name에 `binance-pump-bot` 입력
3. **Public** 또는 **Private** 선택 (Private 추천)
4. `Create repository` 클릭

### 2) 로컬 파일 푸시 (터미널에서 실행)
```bash
cd ~/antigravity/deploy_render

# Git 저장소 초기화
git init
git add .
git commit -m "feat: binance pump alert bot for render"

# GitHub 원격 저장소 연결 및 푸시 (본인 깃허브 주소로 변경)
git branch -M main
git remote add origin https://github.com/<내깃허브아이디>/binance-pump-bot.git
git push -u origin main
```

---

## 4단계: Render.com에 등록 및 배포

1. [Render.com](https://render.com) 접속 및 로그인 (GitHub 계정으로 로그인 권장)
2. 대시보드 우측 상단 **`New +`** 버튼 클릭 → **`Web Service`** 선택
3. **`Build and deploy from a Git repository`** 선택 후 `Next`
4. 방금 올린 **`binance-pump-bot`** 저장소 선택 (`Connect`)
5. 설정 입력:
   - **Name:** `binance-pump-bot`
   - **Region:** ⭐ **`Singapore (Southeast Asia)`** (반드시 싱가포르 선택! 미국 선택 시 바이낸스 차단됨)
   - **Branch:** `main`
   - **Root Directory:** 비워둠 (공란)
   - **Runtime:** `Python 3`
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `python bot.py`
   - **Instance Type:** `Free`
6. 하단 **`Environment Variables`** (환경변수) 추가:
   - `TELEGRAM_BOT_TOKEN` : 발급받은 텔레그램 봇 토큰
   - `TELEGRAM_CHAT_ID` : 내 텔레그램 Chat ID 숫자
   - `SCAN_INTERVAL_MIN` : `15` (15분마다 스캔)
   - `MIN_SCORE_NOTIFY` : `3.0` (점수 3.0 이상만 알림)
   - `COOLDOWN_HOURS` : `4.0` (동일 종목 4시간 재알림 방지)
7. 맨 아래 **`Deploy Web Service`** 클릭!

---

## 5단계: 배포 완료 확인 및 슬립 방지 (무료 티어 유지 팁)

1. 배포가 시작되면 약 1~2분 뒤 로그에 `🚀 Render Healthcheck 웹서버 시작: port 10000` 문구가 뜨고, 텔레그램으로 **`[Binance Alert Bot 가동]`** 알림이 옵니다.
2. Render 무료 웹 서비스는 15분 동안 외부 방문자가 없으면 잠에 듭니다(Sleep). 이를 방지하려면:
   - Render 대시보드 상단에 생성된 내 웹서비스 URL 복사 (예: `https://binance-pump-bot.onrender.com`)
   - 무료 핑 서비스인 [UptimeRobot](https://uptimerobot.com)에 가입
   - **Add New Monitor** → Monitor Type: `HTTP(s)` → 내 URL 입력 → Interval `5분` 설정
   - 이렇게 등록해두면 UptimeRobot이 5분마다 주소로 접속을 찔러주므로 **24시간 365일 무료로 절대 꺼지지 않고 스캔이 돌아갑니다!**
