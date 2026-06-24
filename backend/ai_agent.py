from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import aiohttp

from backend.config import Settings, reveal_secret

try:
    from openai import AsyncOpenAI
except ImportError:  # pragma: no cover - handled at runtime when dependencies are absent
    AsyncOpenAI = None  # type: ignore[assignment]


logger = logging.getLogger(__name__)
JsonDict = dict[str, Any]


class MarketAnalystAI:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._api_key = reveal_secret(settings.openai_api_key)
        self._client = AsyncOpenAI(api_key=self._api_key) if settings.ai_provider == "openai" and AsyncOpenAI and self._api_key else None

    async def complete(self, user_message: str, telemetry: JsonDict) -> str:
        clean_message = user_message.strip()
        if not clean_message:
            return "Send a market, risk, or execution question and I will analyze the live telemetry snapshot."

        if self.settings.ai_provider == "ollama":
            ollama = await self._ollama_complete(clean_message, telemetry)
            if ollama:
                return ollama

        if self._client is None:
            return self._local_analysis(clean_message, telemetry)

        system = self._system_context()
        telemetry_context = self._telemetry_context(telemetry)
        prompt = (
            "User question:\n"
            f"{clean_message}\n\n"
            "Live system telemetry JSON:\n"
            f"{telemetry_context}\n\n"
            "Produce concise, actionable analysis grounded only in the telemetry. "
            "Flag data gaps, risk lockouts, and execution hazards explicitly."
        )
        try:
            response = await self._client.responses.create(
                model=self.settings.openai_model,
                instructions=system,
                input=prompt,
                max_output_tokens=900,
                store=False,
            )
            text = getattr(response, "output_text", None)
            if text:
                return str(text).strip()
            return self._extract_response_text(response) or self._local_analysis(clean_message, telemetry)
        except Exception as exc:  # noqa: BLE001 - API failures should degrade gracefully for the dashboard
            logger.warning("OpenAI analysis request failed: %s", exc, exc_info=True)
            fallback = self._local_analysis(clean_message, telemetry)
            return f"OpenAI analysis is temporarily unavailable, so I used the local telemetry model.\n\n{fallback}"

    async def analyze_files(self, question: str, telemetry: JsonDict, files: list[JsonDict]) -> str:
        clean_question = question.strip() or "Analyze these files for trading-system risks and improvement opportunities."
        context = self._file_context(files)
        if self.settings.ai_provider == "ollama":
            prompt = (
                f"{clean_question}\n\n"
                f"Telemetry:\n{self._telemetry_context(telemetry)}\n\n"
                f"File context:\n{context}"
            )
            response = await self._ollama_prompt(prompt)
            if response:
                return response
        if self._client is not None:
            try:
                response = await self._client.responses.create(
                    model=self.settings.openai_model,
                    instructions=(
                        "Analyze the supplied project files as a senior trading-systems reviewer. "
                        "Focus on correctness, risk, reliability, and missing verification. "
                        "Do not promise profits."
                    ),
                    input=f"Question:\n{clean_question}\n\nTelemetry:\n{self._telemetry_context(telemetry)}\n\nFiles:\n{context}",
                    max_output_tokens=1100,
                    store=False,
                )
                text = getattr(response, "output_text", None)
                if text:
                    return str(text).strip()
            except Exception:
                logger.warning("OpenAI file analysis failed; falling back to local analysis", exc_info=True)
        return self._local_file_analysis(clean_question, files, telemetry)

    def _system_context(self) -> str:
        return (
            "You are MarketAnalystAI, a quantitative trading systems analyst embedded in a MEXC spot trading engine. "
            "Use the provided telemetry: current tier position, balance sheets, trailing indicators, order-book imbalance, "
            "Kelly sizing, risk locks, and execution logs. Do not invent balances, prices, or fills. "
            "Do not claim certainty. When the engine is in paper or disabled mode, say so. "
            "Return practical operational insight for the user, not generic market commentary."
        )

    async def _ollama_complete(self, user_message: str, telemetry: JsonDict) -> str:
        prompt = (
            f"{self._system_context()}\n\n"
            f"User question:\n{user_message}\n\n"
            f"Live telemetry:\n{self._telemetry_context(telemetry)}"
        )
        return await self._ollama_prompt(prompt)

    async def _ollama_prompt(self, prompt: str) -> str:
        url = f"{self.settings.ollama_base_url.rstrip('/')}/api/generate"
        payload = {
            "model": self.settings.ollama_model,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0.2, "num_predict": 900},
        }
        try:
            timeout = aiohttp.ClientTimeout(total=45)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, json=payload) as response:
                    if response.status >= 400:
                        return ""
                    data = await response.json()
                    return str(data.get("response") or "").strip()
        except Exception:
            logger.warning("Ollama analysis unavailable; falling back to local analysis", exc_info=True)
            return ""

    @staticmethod
    def _telemetry_context(telemetry: JsonDict) -> str:
        compact = {
            "symbol": telemetry.get("symbol"),
            "mode": telemetry.get("mode"),
            "system_balance": telemetry.get("system_balance"),
            "tier_progression": telemetry.get("tier_progression"),
            "active_trade_metrics": telemetry.get("active_trade_metrics"),
            "market": telemetry.get("market"),
            "risk": telemetry.get("risk"),
            "research": telemetry.get("research"),
            "portfolio": telemetry.get("portfolio"),
            "websocket": telemetry.get("websocket"),
            "execution_logs": telemetry.get("execution_logs", [])[-12:],
            "timestamp": telemetry.get("timestamp"),
        }
        return json.dumps(compact, indent=2, sort_keys=True)[:14000]

    @staticmethod
    def _file_context(files: list[JsonDict]) -> str:
        chunks: list[str] = []
        for file in files:
            path = file.get("path") or "unknown"
            text = str(file.get("content") or "")
            chunks.append(f"--- FILE: {path} ---\n{text[:12000]}")
        return "\n\n".join(chunks)[:30000]

    @staticmethod
    def _extract_response_text(response: Any) -> str:
        try:
            chunks: list[str] = []
            for item in getattr(response, "output", []) or []:
                for content in getattr(item, "content", []) or []:
                    text = getattr(content, "text", None)
                    if text:
                        chunks.append(str(text))
            return "\n".join(chunks).strip()
        except Exception:
            return ""

    def _local_analysis(self, user_message: str, telemetry: JsonDict) -> str:
        balance = telemetry.get("system_balance") or {}
        tier = telemetry.get("tier_progression") or {}
        trade = telemetry.get("active_trade_metrics") or {}
        signal = trade.get("last_signal") or {}
        market = telemetry.get("market") or {}
        indicators = market.get("indicators") or {}
        order_book = market.get("order_book") or {}
        websocket = telemetry.get("websocket") or {}
        research = telemetry.get("research") or {}
        portfolio = telemetry.get("portfolio") or {}

        equity = float(balance.get("total_equity_usdt") or 0.0)
        current_tier = tier.get("current_tier") or "UNKNOWN"
        next_delta = float(tier.get("progression_delta_usdt") or 0.0)
        locked = bool(tier.get("trading_locked"))
        daily_drawdown = float(tier.get("daily_drawdown_pct") or 0.0) * 100.0
        action = signal.get("action") or "HOLD"
        confidence = float(signal.get("confidence") or 0.0) * 100.0
        z_score = float(indicators.get("z_score") or 0.0)
        imbalance = float(order_book.get("imbalance") or 0.0)
        mode = telemetry.get("mode") or self.settings.trading_mode

        public_ws = (websocket.get("public") or {}).get("connected")
        private_ws = (websocket.get("private") or {}).get("connected")
        connection_line = f"Public websocket connected: {public_ws}. Private websocket connected: {private_ws}."
        research_line = (
            f"Research profile loaded from {research.get('interval')} calibration with "
            f"{research.get('candle_count')} candles."
            if research.get("profile_loaded")
            else "No research profile is loaded yet; run the calibration command before treating signals as tuned."
        )

        if locked:
            primary = (
                f"Trading is locked until {tier.get('locked_until')} because the drawdown circuit breaker is active. "
                f"Daily drawdown is {daily_drawdown:.2f}% against a {float(self.settings.hard_daily_drawdown_limit_pct) * 100:.2f}% hard limit."
            )
        elif action in {"BUY", "SELL"}:
            primary = (
                f"The engine currently emits {action} with {confidence:.1f}% confidence. "
                f"Recommended notional is {float(signal.get('recommended_notional_usdt') or 0.0):.2f} USDT, "
                f"with Kelly fraction {float(signal.get('kelly_fraction') or 0.0) * 100:.2f}%."
            )
        else:
            primary = (
                f"The engine is holding. Latest signal reason: {signal.get('reason') or 'no active edge'}."
            )

        return (
            f"{primary}\n\n"
            f"Capital is {equity:.2f} USDT in {current_tier}; progression delta is {next_delta:.2f} USDT. "
            f"Market state: z-score {z_score:.2f}, top-5 OBI {imbalance:.3f}. "
            f"Portfolio symbols tracked: {', '.join(portfolio.get('symbols', [])[:12]) if portfolio.get('symbols') else telemetry.get('symbol')}. "
            f"Runtime mode is {mode}, so live order placement is {'eligible only after live-mode safeguards pass' if mode == 'live' else 'not active'}.\n\n"
            f"{connection_line} {research_line}\n\n"
            f"Question interpreted: {user_message}"
        )

    def _local_file_analysis(self, question: str, files: list[JsonDict], telemetry: JsonDict) -> str:
        if not files:
            return "No readable files were supplied. Send paths to `/api/analyze-files` or place files inside the configured analysis root."

        total_lines = 0
        risks: list[str] = []
        highlights: list[str] = []
        for file in files:
            path = str(file.get("path") or "unknown")
            content = str(file.get("content") or "")
            lines = content.splitlines()
            total_lines += len(lines)
            lowered = content.lower()
            if "live_trading_confirmation" in lowered:
                highlights.append(f"{path}: live trading is gated by explicit confirmation.")
            if "todo" in lowered or "placeholder" in lowered:
                risks.append(f"{path}: contains TODO/placeholder language that should be resolved before live use.")
            if "except exception" in lowered or "except:" in lowered:
                risks.append(f"{path}: broad exception handling exists; confirm it records enough telemetry.")
            if "api_key" in lowered or "secret" in lowered:
                highlights.append(f"{path}: handles credentials or secrets; keep it out of public logs.")
            if "drawdown" in lowered:
                highlights.append(f"{path}: includes drawdown logic.")

        portfolio = telemetry.get("portfolio") or {}
        symbols = portfolio.get("symbols") or [telemetry.get("symbol")]
        response = [
            "Free local file analysis complete.",
            "",
            f"Question: {question}",
            f"Files read: {len(files)}. Lines scanned: {total_lines}. Symbols tracked: {', '.join([str(s) for s in symbols if s])}.",
            "",
            "What looks strong:",
        ]
        response.extend([f"- {item}" for item in highlights[:8]] or ["- The submitted files were readable and structured enough for local inspection."])
        response.append("")
        response.append("What needs attention before real money:")
        response.extend([f"- {item}" for item in risks[:10]] or ["- No obvious placeholder/TODO markers were found in the submitted file slice."])
        response.append("- This local analyzer is free and deterministic, but it is not a substitute for backtesting, forward paper trading, and code review.")
        return "\n".join(response)
