from __future__ import annotations

import asyncio
import copy
import secrets
import logging
import threading
import time
from collections import deque
from contextlib import asynccontextmanager
from decimal import Decimal
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from backend.advanced_engine import parse_decimal, utc_now
from backend.ai_agent import MarketAnalystAI
from backend.config import Settings, get_settings
from backend.mexc_pro import MexcAPIError, MexcRESTClient, MexcRateLimitError, MexcWebSocketSupervisor
from backend.portfolio import PortfolioAction, PortfolioTradingEngine
from backend.research import run_research_for_symbols
from backend.config import reveal_secret


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("projectmile3")

ROOT_DIR = Path(__file__).resolve().parent
DASHBOARD_PATH = ROOT_DIR / "frontend" / "dashboard.html"


class TelemetryHub:
    def __init__(self, settings: Settings) -> None:
        self._lock = threading.RLock()
        self._snapshot: dict[str, Any] = {
            "app": settings.app_name,
            "symbol": ",".join(settings.trading_symbols),
            "mode": settings.trading_mode,
            "timestamp": utc_now().isoformat(),
            "system_balance": {"total_equity_usdt": float(settings.paper_starting_equity_usdt)},
            "tier_progression": {
                "current_tier": settings.capital_tiers[0].name,
                "current_tier_number": settings.capital_tiers[0].tier,
                "progression_delta_usdt": float(settings.capital_tiers[1].required_equity_usdt - settings.paper_starting_equity_usdt),
                "trading_locked": False,
                "reason": "initializing",
            },
            "active_trade_metrics": {
                "last_signal": {
                    "action": "HOLD",
                    "confidence": 0.0,
                    "reason": "runtime_initializing",
                    "recommended_notional_usdt": 0.0,
                    "kelly_fraction": 0.0,
                }
            },
            "market": {},
            "websocket": {},
            "execution_logs": [],
        }
        self._history: deque[dict[str, Any]] = deque(maxlen=720)
        self._history.append(self._compact_history_point(self._snapshot))

    def update(self, snapshot: dict[str, Any]) -> None:
        with self._lock:
            self._snapshot = copy.deepcopy(snapshot)
            self._history.append(self._compact_history_point(snapshot))

    def get(self) -> dict[str, Any]:
        with self._lock:
            return copy.deepcopy(self._snapshot)

    def get_history(self) -> list[dict[str, Any]]:
        with self._lock:
            return copy.deepcopy(list(self._history))

    @staticmethod
    def _compact_history_point(snapshot: dict[str, Any]) -> dict[str, Any]:
        balance = snapshot.get("system_balance") or {}
        trade = snapshot.get("active_trade_metrics") or {}
        signal = trade.get("last_signal") or {}
        tier = snapshot.get("tier_progression") or {}
        market = snapshot.get("market") or {}
        book = market.get("order_book") or {}
        indicators = market.get("indicators") or {}
        return {
            "timestamp": snapshot.get("timestamp"),
            "equity_usdt": balance.get("total_equity_usdt"),
            "mark_price": trade.get("mark_price") or book.get("mid_price"),
            "signal": signal.get("action"),
            "confidence": signal.get("confidence"),
            "drawdown_pct": tier.get("daily_drawdown_pct"),
            "obi": book.get("imbalance"),
            "z_score": indicators.get("z_score"),
            "locked": tier.get("trading_locked"),
        }


class TradingRuntime:
    def __init__(self, settings: Settings, telemetry: TelemetryHub) -> None:
        self.settings = settings
        self.telemetry = telemetry
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stop_event: asyncio.Event | None = None
        self._started = threading.Event()
        self._last_live_execution_monotonic = 0.0

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._thread_main, name="mexc-trading-runtime", daemon=True)
        self._thread.start()
        if not self._started.wait(timeout=15):
            raise RuntimeError("trading runtime did not start within 15 seconds")

    def stop(self) -> None:
        if self._loop and self._stop_event:
            self._loop.call_soon_threadsafe(self._stop_event.set)
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=20)
        self._thread = None
        self._loop = None
        self._stop_event = None
        self._started = threading.Event()

    def restart(self) -> None:
        self.stop()
        self.start()

    def _thread_main(self) -> None:
        loop = asyncio.new_event_loop()
        self._loop = loop
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._run())
        except Exception:
            logger.exception("Trading runtime terminated unexpectedly")
        finally:
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.close()

    async def _run(self) -> None:
        stop_event = asyncio.Event()
        self._stop_event = stop_event
        engine = PortfolioTradingEngine(self.settings)
        supervisor: MexcWebSocketSupervisor | None = None
        self.telemetry.update(engine.telemetry())
        self._started.set()

        async with MexcRESTClient(self.settings) as rest_client:
            async def on_market_data(message: dict[str, Any]) -> None:
                engine.on_order_book(message)
                if self.settings.live_trading_armed:
                    await self._execute_live_portfolio_actions(rest_client, engine, engine.last_actions)
                self.telemetry.update(engine.telemetry(supervisor.health() if supervisor else None))

            async def on_private_data(message: dict[str, Any]) -> None:
                engine.on_private_update(message)
                self.telemetry.update(engine.telemetry(supervisor.health() if supervisor else None))

            supervisor = MexcWebSocketSupervisor(
                self.settings,
                rest_client,
                on_market_data=on_market_data,
                on_private_data=on_private_data,
            )
            tasks = [
                asyncio.create_task(supervisor.run(stop_event), name="mexc-websocket-supervisor"),
                asyncio.create_task(self._rest_poll_loop(rest_client, engine, supervisor, stop_event), name="mexc-rest-poll-loop"),
            ]
            try:
                await stop_event.wait()
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                self.telemetry.update(engine.telemetry(supervisor.health()))

    async def _rest_poll_loop(
        self,
        rest_client: MexcRESTClient,
        engine: PortfolioTradingEngine,
        supervisor: MexcWebSocketSupervisor,
        stop_event: asyncio.Event,
    ) -> None:
        while not stop_event.is_set():
            wait_seconds = self.settings.rest_poll_interval_seconds
            try:
                for symbol in self.settings.trading_symbols:
                    ticker = await rest_client.ticker_price(symbol)
                    price = parse_decimal(ticker.get("price"))
                    if price > 0:
                        engine.set_last_price(symbol, price)

                    depth = await rest_client.depth_snapshot(symbol, limit=5)
                    engine.refresh_depth_snapshot(symbol, depth)

                if self.settings.has_mexc_credentials:
                    account = await rest_client.account_information()
                    engine.refresh_account(account)
                elif self.settings.trading_mode == "live" and not self.settings.live_trading_armed:
                    engine.record_event(
                        "live_trading_not_armed",
                        {
                            "required": "set LIVE_TRADING_CONFIRMATION=I_UNDERSTAND_LIVE_MEXC_TRADING",
                            "mode": self.settings.trading_mode,
                        },
                    )

                if self.settings.live_trading_armed:
                    engine.rebalance_if_due(force=True)
                    await self._execute_live_portfolio_actions(rest_client, engine, engine.last_actions)

                self.telemetry.update(engine.telemetry(supervisor.health()))
            except MexcRateLimitError as exc:
                wait_seconds = max(wait_seconds, exc.retry_after or 10.0)
                engine.record_event("mexc_rate_limit", {"status": exc.status, "retry_after": wait_seconds})
                self.telemetry.update(engine.telemetry(supervisor.health()))
            except MexcAPIError as exc:
                engine.record_event("mexc_api_error", {"message": str(exc), "status": exc.status})
                self.telemetry.update(engine.telemetry(supervisor.health()))
            except Exception as exc:  # noqa: BLE001 - long-running supervisor must isolate transient faults
                engine.record_event("runtime_poll_error", {"message": str(exc), "type": type(exc).__name__})
                self.telemetry.update(engine.telemetry(supervisor.health()))

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=wait_seconds)
            except asyncio.TimeoutError:
                continue

    async def _execute_live_portfolio_actions(
        self,
        rest_client: MexcRESTClient,
        engine: PortfolioTradingEngine,
        actions: list[PortfolioAction],
    ) -> None:
        if not self.settings.live_trading_armed:
            return
        if not self.settings.has_mexc_credentials:
            engine.record_event("live_execution_blocked", {"reason": "missing_mexc_credentials"})
            return
        tier = (engine.telemetry().get("tier_progression") or {})
        if tier.get("trading_locked"):
            engine.record_event("live_execution_blocked", {"reason": "tier_lockout"})
            return
        now = time.monotonic()
        if now - self._last_live_execution_monotonic < 60:
            return
        eligible = [
            action for action in actions
            if action.confidence >= self.settings.min_live_execution_confidence
            and action.notional_usdt >= self.settings.minimum_trade_notional_usdt
            and action.action in {"BUY", "SELL"}
        ]
        if not eligible:
            return
        self._last_live_execution_monotonic = now
        for action in eligible:
            try:
                if action.action == "BUY":
                    order = await rest_client.place_order(
                        symbol=action.symbol,
                        side="BUY",
                        order_type="MARKET",
                        quote_order_qty=action.notional_usdt.quantize(Decimal("0.01"), rounding="ROUND_DOWN"),
                        client_order_id=f"pm3_buy_{action.symbol}_{int(time.time() * 1000)}",
                    )
                else:
                    mark = engine.engines[action.symbol].mark_price()
                    quantity = Decimal("0") if mark <= 0 else action.notional_usdt / mark
                    if quantity <= 0:
                        engine.record_event("live_execution_blocked", {"symbol": action.symbol, "reason": "no_quantity_to_sell"})
                        continue
                    order = await rest_client.place_order(
                        symbol=action.symbol,
                        side="SELL",
                        order_type="MARKET",
                        quantity=quantity.quantize(Decimal("0.00000001"), rounding="ROUND_DOWN"),
                        client_order_id=f"pm3_sell_{action.symbol}_{int(time.time() * 1000)}",
                    )
                engine.record_event("live_order_submitted", {"symbol": action.symbol, "action": action.action, "response": order})
            except Exception as exc:  # noqa: BLE001 - order failures belong in telemetry
                engine.record_event("live_order_failed", {"symbol": action.symbol, "action": action.action, "message": str(exc), "type": type(exc).__name__})


class ChatRequest(BaseModel):
    message: str = Field(min_length=1, max_length=4000)


class ChatResponse(BaseModel):
    response: str
    telemetry_timestamp: str | None = None


class FileAnalysisRequest(BaseModel):
    question: str = Field(default="Analyze these files for trading-system risks and improvements.", max_length=4000)
    paths: list[str] = Field(default_factory=list, max_length=30)


class TrainRequest(BaseModel):
    symbols: list[str] | None = Field(default=None, max_length=30)
    interval: str | None = Field(default=None, max_length=8)
    days: int | None = Field(default=None, ge=1, le=365)
    restart_runtime: bool = True


class TrainingManager:
    def __init__(self, settings: Settings, runtime: TradingRuntime, telemetry: TelemetryHub) -> None:
        self.settings = settings
        self.runtime = runtime
        self.telemetry = telemetry
        self._lock = asyncio.Lock()
        self._status: dict[str, Any] = {
            "state": "idle",
            "started_at": None,
            "finished_at": None,
            "message": "No training run active.",
            "profiles": [],
            "open_symbols": [],
            "error": None,
        }

    def status(self) -> dict[str, Any]:
        return copy.deepcopy(self._status)

    async def start(self, request: TrainRequest) -> dict[str, Any]:
        if self._lock.locked() or self._status.get("state") in {"queued", "running"}:
            return self.status()
        self._status = {
            "state": "queued",
            "started_at": utc_now().isoformat(),
            "finished_at": None,
            "message": "Training queued.",
            "profiles": [],
            "open_symbols": [],
            "error": None,
        }
        asyncio.create_task(self._run(request), name="projectmile3-training")
        return self.status()

    async def _run(self, request: TrainRequest) -> None:
        async with self._lock:
            symbols = [symbol.strip().upper() for symbol in request.symbols or list(self.settings.trading_symbols) if symbol.strip()]
            interval = request.interval or self.settings.research_default_interval
            days = request.days or self.settings.research_default_days
            self._status.update(
                {
                    "state": "running",
                    "started_at": utc_now().isoformat(),
                    "finished_at": None,
                    "message": f"Training {', '.join(symbols)} on {interval} candles for {days} day(s).",
                    "profiles": [],
                    "open_symbols": [],
                    "error": None,
                }
            )
            try:
                if request.restart_runtime:
                    self._status["message"] = "Stopping trading runtime for clean training."
                    await asyncio.to_thread(self.runtime.stop)
                profiles = await run_research_for_symbols(self.settings, symbols=symbols, interval=interval, days=days)
                open_symbols = [
                    str(profile.get("symbol"))
                    for profile in profiles
                    if (profile.get("trade_permission") or {}).get("paper_trading_allowed")
                ]
                self._status.update(
                    {
                        "profiles": [
                            {
                                "symbol": profile.get("symbol"),
                                "strategy": (profile.get("strategy_parameters") or {}).get("strategy_type"),
                                "return_pct": (profile.get("backtest") or {}).get("total_return_pct"),
                                "sharpe": (profile.get("backtest") or {}).get("sharpe"),
                                "trade_count": (profile.get("backtest") or {}).get("trade_count"),
                                "gate_open": (profile.get("trade_permission") or {}).get("paper_trading_allowed"),
                                "reason": (profile.get("trade_permission") or {}).get("reason"),
                            }
                            for profile in profiles
                        ],
                        "open_symbols": open_symbols,
                    }
                )
                if len(open_symbols) < self.settings.training_min_open_symbols:
                    self._status["message"] = (
                        "Training finished, but not enough symbols passed the trade gate. "
                        "Runtime will restart with cash-protection gates active."
                    )
                else:
                    self._status["message"] = (
                        f"Training finished. Trade gate open for {', '.join(open_symbols)}. "
                        "Runtime restarting with refreshed profiles."
                    )
                if request.restart_runtime:
                    await asyncio.to_thread(self.runtime.start)
                self._status["state"] = "complete"
                self._status["finished_at"] = utc_now().isoformat()
            except Exception as exc:  # noqa: BLE001 - surface training failures to dashboard
                logger.exception("Training failed")
                self._status.update(
                    {
                        "state": "failed",
                        "finished_at": utc_now().isoformat(),
                        "message": "Training failed. Runtime is being restarted in protected mode.",
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
                if request.restart_runtime:
                    await asyncio.to_thread(self.runtime.start)


def verify_api_access(request: Request) -> None:
    expected = reveal_secret(request.app.state.settings.app_access_token)
    if not expected:
        return
    supplied = request.headers.get("x-mile3-token") or request.query_params.get("token")
    if not supplied or not secrets.compare_digest(supplied, expected):
        raise HTTPException(status_code=401, detail="Invalid or missing Mile3 access token")


def collect_analysis_files(paths: list[str], settings: Settings) -> list[dict[str, Any]]:
    allowed_suffixes = {".py", ".md", ".txt", ".yaml", ".yml", ".html", ".json", ".toml", ".css", ".js"}
    blocked_names = {".env", ".env.local", ".env.production"}
    blocked_parts = {".venv", "venv", "__pycache__", ".pytest_cache", "data"}
    root = ROOT_DIR.resolve()
    candidates: list[Path]
    if paths:
        candidates = [(root / path).resolve() if not Path(path).is_absolute() else Path(path).resolve() for path in paths]
    else:
        candidates = [
            path for path in root.rglob("*")
            if path.is_file()
            and path.suffix.lower() in allowed_suffixes
            and not any(part in blocked_parts for part in path.parts)
        ][:30]

    files: list[dict[str, Any]] = []
    remaining_chars = settings.max_file_analysis_chars
    for path in candidates:
        if remaining_chars <= 0:
            break
        try:
            path.relative_to(root)
        except ValueError:
            continue
        if path.name in blocked_names or path.suffix.lower() not in allowed_suffixes:
            continue
        if any(part in blocked_parts for part in path.parts):
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        slice_text = text[:remaining_chars]
        remaining_chars -= len(slice_text)
        files.append({"path": str(path.relative_to(root)), "content": slice_text, "truncated": len(slice_text) < len(text)})
    return files


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    if settings.live_trading_armed and not reveal_secret(settings.app_access_token):
        raise RuntimeError("APP_ACCESS_TOKEN is required when live trading is armed.")
    telemetry = TelemetryHub(settings)
    ai_agent = MarketAnalystAI(settings)
    runtime = TradingRuntime(settings, telemetry)
    trainer = TrainingManager(settings, runtime, telemetry)
    app.state.settings = settings
    app.state.telemetry = telemetry
    app.state.ai_agent = ai_agent
    app.state.runtime = runtime
    app.state.trainer = trainer
    runtime.start()
    try:
        yield
    finally:
        runtime.stop()


app = FastAPI(title="ProjectMile3 MEXC Quant Engine", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


@app.get("/", include_in_schema=False)
async def dashboard() -> FileResponse:
    if not DASHBOARD_PATH.exists():
        raise HTTPException(status_code=404, detail="dashboard.html not found")
    return FileResponse(DASHBOARD_PATH)


@app.get("/healthz")
async def healthz() -> dict[str, Any]:
    snapshot = app.state.telemetry.get()
    return {
        "ok": True,
        "timestamp": utc_now().isoformat(),
        "symbol": snapshot.get("symbol"),
        "mode": snapshot.get("mode"),
    }


@app.get("/api/telemetry")
async def telemetry(request: Request) -> JSONResponse:
    verify_api_access(request)
    return JSONResponse(app.state.telemetry.get())


@app.get("/api/history")
async def history(request: Request) -> JSONResponse:
    verify_api_access(request)
    return JSONResponse({"points": app.state.telemetry.get_history()})


@app.get("/api/research")
async def research(request: Request) -> JSONResponse:
    verify_api_access(request)
    snapshot = app.state.telemetry.get()
    return JSONResponse(snapshot.get("research") or {})


@app.post("/api/chat", response_model=ChatResponse)
async def chat(request: ChatRequest, raw_request: Request) -> ChatResponse:
    verify_api_access(raw_request)
    snapshot = app.state.telemetry.get()
    response = await app.state.ai_agent.complete(request.message, snapshot)
    return ChatResponse(response=response, telemetry_timestamp=snapshot.get("timestamp"))


@app.post("/api/analyze-files", response_model=ChatResponse)
async def analyze_files(request: FileAnalysisRequest, raw_request: Request) -> ChatResponse:
    verify_api_access(raw_request)
    snapshot = app.state.telemetry.get()
    files = collect_analysis_files(request.paths, app.state.settings)
    response = await app.state.ai_agent.analyze_files(request.question, snapshot, files)
    return ChatResponse(response=response, telemetry_timestamp=snapshot.get("timestamp"))


@app.get("/api/training")
async def training_status(request: Request) -> JSONResponse:
    verify_api_access(request)
    return JSONResponse(app.state.trainer.status())


@app.post("/api/train")
async def train(request: TrainRequest, raw_request: Request) -> JSONResponse:
    verify_api_access(raw_request)
    status = await app.state.trainer.start(request)
    return JSONResponse(status)
