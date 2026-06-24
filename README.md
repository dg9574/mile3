# ProjectMile3 MEXC Quant Engine

Professional spot-trading research and execution console for MEXC, built for FastAPI and Render.

This system is designed to be tested in paper mode first. It cannot guarantee income, cannot guarantee wins, and should not be switched to live trading until the paper profile has proven itself under your own monitoring.

## Local Start

```powershell
cd C:\Users\PC\Desktop\ProjectMile3
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install --upgrade pip
pip install -r requirements.txt
Copy-Item .env.example .env
```

Edit `.env` and set:

```text
TRADING_SYMBOL=BTCUSDT
TRADING_SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT,BNBUSDT
TRADING_MODE=paper
APP_ACCESS_TOKEN=make-this-a-long-random-password
AI_PROVIDER=local
OPENAI_API_KEY=optional_only_if_you_choose_AI_PROVIDER=openai
MEXC_API_KEY=optional_for_account_streams
MEXC_SECRET_KEY=optional_for_account_streams
```

The default `AI_PROVIDER=local` costs nothing. It powers telemetry analysis and the dashboard's free project-file analyzer without calling OpenAI.

Optional local LLM mode:

```text
AI_PROVIDER=ollama
OLLAMA_BASE_URL=http://127.0.0.1:11434
OLLAMA_MODEL=llama3.1:8b
```

Run tests:

```powershell
python -m pytest -q
```

Run the app:

```powershell
uvicorn main:app --host 0.0.0.0 --port 8000
```

Open:

```text
http://127.0.0.1:8000
```

Press `Access Token`, enter `APP_ACCESS_TOKEN`, then press `Train Mile3`. The dashboard asks for symbols, interval, and lookback days, trains research profiles from MEXC candles, stops the runtime during training, and restarts it with fresh trade gates.

You can still train from PowerShell:

```powershell
python scripts\train_research.py --symbols BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT,BNBUSDT --interval 5m --days 30
```

## API

- `GET /healthz` runtime health
- `GET /api/telemetry` current state
- `GET /api/history` chart history
- `GET /api/research` active calibration profile
- `POST /api/chat` AI analyst chat
- `POST /api/analyze-files` free local project-file analysis
- `GET /api/training` training status
- `POST /api/train` start dashboard training

## Live Trading Guard

Live orders are blocked unless all conditions are true:

```text
TRADING_MODE=live
LIVE_TRADING_CONFIRMATION=I_UNDERSTAND_LIVE_MEXC_TRADING
APP_ACCESS_TOKEN=make-this-a-long-random-password
MEXC_API_KEY=...
MEXC_SECRET_KEY=...
```

Keep the first phase in paper mode. Treat the tier plan as a validation ladder:

```text
100 USDT -> 1000 USDT -> 3000 USDT -> 5000 USDT -> 10000 USDT
```

The engine also enforces a 3% daily drawdown lockout by default.

## Render

Create a Render web service from this repo. The included `render.yaml` uses:

```text
buildCommand: pip install --upgrade pip && pip install -r requirements.txt
startCommand: python scripts/render_start.py
```

Set environment variables in Render from `.env.example`. Store `APP_ACCESS_TOKEN`, `MEXC_API_KEY`, `MEXC_SECRET_KEY`, and `LIVE_TRADING_CONFIRMATION` as secret environment variables in Render, not in git.

The included Render startup boots the website quickly. Training is started from the dashboard by pressing `Train Mile3`. If `TRADING_MODE=live` and the live confirmation is set, the runtime restarts after training and can submit live market orders only for symbols whose research gate is open.
