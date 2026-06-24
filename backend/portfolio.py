from __future__ import annotations

import json
import os
import tempfile
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from backend.advanced_engine import (
    AdvancedTradingEngine,
    ExecutionLogEntry,
    JsonDict,
    MilestoneVerifier,
    decimal_float,
    parse_decimal,
    utc_now,
)
from backend.config import Settings, split_symbol


@dataclass(slots=True)
class PortfolioAction:
    symbol: str
    action: str
    notional_usdt: Decimal
    confidence: Decimal
    reason: str

    def as_dict(self) -> JsonDict:
        return {
            "symbol": self.symbol,
            "action": self.action,
            "notional_usdt": decimal_float(self.notional_usdt),
            "confidence": decimal_float(self.confidence),
            "reason": self.reason,
        }


class PortfolioStateStore:
    def __init__(self, path: str) -> None:
        self.path = Path(path)

    def load(self) -> JsonDict:
        try:
            if self.path.exists():
                return json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        return {}

    def save(self, state: JsonDict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(prefix=f".{self.path.name}.", dir=str(self.path.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(state, handle, indent=2, sort_keys=True)
                handle.write("\n")
            os.replace(tmp_name, self.path)
        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)


class PortfolioTradingEngine:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.symbols = settings.trading_symbols
        self.quote_asset = split_symbol(self.symbols[0])[1]
        self.store = PortfolioStateStore(settings.portfolio_state_path)
        stored = self.store.load()
        self.engines: dict[str, AdvancedTradingEngine] = {
            symbol: AdvancedTradingEngine(
                settings,
                symbol=symbol,
                state_path=self._engine_state_path(symbol),
                starting_quote_balance=Decimal("0"),
            )
            for symbol in self.symbols
        }
        self.quote_balance = parse_decimal(stored.get("quote_balance"), settings.paper_starting_equity_usdt)
        self.positions: dict[str, JsonDict] = stored.get("positions", {}) if isinstance(stored.get("positions"), dict) else {}
        self.account_balances: dict[str, dict[str, Decimal]] = {}
        self.milestones = MilestoneVerifier(settings, stored.get("milestones"))
        self.execution_log: deque[ExecutionLogEntry] = deque(maxlen=300)
        for row in stored.get("execution_log", [])[-80:]:
            try:
                timestamp = datetime.fromisoformat(row["timestamp"])
                self.execution_log.append(ExecutionLogEntry(timestamp=timestamp, event=row["event"], details=row["details"]))
            except (KeyError, TypeError, ValueError):
                continue
        self.last_rebalance_monotonic = 0.0
        self.last_actions: list[PortfolioAction] = []
        self.started_at = utc_now()

    def _engine_state_path(self, symbol: str) -> str:
        path = Path(self.settings.portfolio_state_path)
        return str(path.parent / f"engine_state_{symbol}.json")

    def record_event(self, event: str, details: JsonDict) -> None:
        self.execution_log.append(ExecutionLogEntry(timestamp=utc_now(), event=event, details=details))

    def on_order_book(self, update: JsonDict) -> None:
        symbol = str(update.get("symbol") or "").upper()
        if symbol in self.engines:
            self.engines[symbol].on_order_book(update)
            self.rebalance_if_due()

    def set_last_price(self, symbol: str, price: Decimal) -> None:
        engine = self.engines.get(symbol)
        if engine:
            engine.set_last_price(price)

    def refresh_depth_snapshot(self, symbol: str, snapshot: JsonDict) -> None:
        engine = self.engines.get(symbol)
        if engine:
            engine.refresh_depth_snapshot(snapshot)
            self.rebalance_if_due()

    def refresh_account(self, account_payload: JsonDict) -> None:
        for engine in self.engines.values():
            engine.refresh_account(account_payload)
        balances = account_payload.get("balances", [])
        parsed: dict[str, dict[str, Decimal]] = {}
        if isinstance(balances, list):
            for balance in balances:
                asset = str(balance.get("asset") or balance.get("currency") or "").upper()
                if not asset:
                    continue
                free = parse_decimal(balance.get("free") or balance.get("available") or balance.get("balanceAmount"))
                locked = parse_decimal(balance.get("locked") or balance.get("frozen") or balance.get("frozenAmount"))
                if free != 0 or locked != 0 or asset == self.quote_asset:
                    parsed[asset] = {"free": free, "locked": locked}
        self.account_balances = parsed
        self.record_event("account_refreshed", {"asset_count": len(balances) if isinstance(balances, list) else 0})

    def on_private_update(self, message: JsonDict) -> None:
        symbol = str(message.get("symbol") or "").upper()
        if symbol in self.engines:
            self.engines[symbol].on_private_update(message)
        self.record_event("private_update", {"symbol": symbol or "UNKNOWN", "channel": message.get("channel")})

    def rebalance_if_due(self, *, force: bool = False) -> list[PortfolioAction]:
        now = time.monotonic()
        if not force and now - self.last_rebalance_monotonic < self.settings.portfolio_rebalance_interval_seconds:
            return self.last_actions
        self.last_rebalance_monotonic = now
        if self.settings.live_trading_armed:
            self.last_actions = self._rebalance_live() if self.account_balances else []
        else:
            self.last_actions = self._rebalance_paper()
        self._persist()
        return self.last_actions

    def _rebalance_live(self) -> list[PortfolioAction]:
        actions: list[PortfolioAction] = []
        total_equity = self.total_equity_usdt()
        tier_status = self.milestones.register_equity(total_equity)
        if tier_status.trading_locked:
            self.record_event("portfolio_locked", {"reason": tier_status.reason, "locked_until": tier_status.locked_until.isoformat() if tier_status.locked_until else None})
            return actions

        rankings = self.rank_symbols()
        held_symbols = self._live_held_symbols()
        selected_symbols = {
            row["symbol"]
            for row in rankings
            if row["action"] == "BUY" and row["confidence"] >= decimal_float(self.settings.portfolio_min_confidence)
        }
        selected_symbols = set(list(selected_symbols)[: self.settings.max_active_positions])

        for symbol in sorted(held_symbols):
            signal = self.engines[symbol].last_signal
            should_sell = signal.action == "SELL" and signal.confidence >= self.settings.portfolio_rotation_sell_confidence
            should_rotate = selected_symbols and symbol not in selected_symbols and signal.action != "BUY"
            if not (should_sell or should_rotate):
                continue
            mark = self.engines[symbol].mark_price()
            quantity = self._account_total(split_symbol(symbol)[0])
            notional = quantity * mark
            if mark <= 0 or notional < self.settings.minimum_trade_notional_usdt:
                continue
            actions.append(
                PortfolioAction(
                    symbol=symbol,
                    action="SELL",
                    notional_usdt=notional,
                    confidence=signal.confidence,
                    reason="live_sell_signal" if should_sell else "live_rotation",
                )
            )

        quote_free = self._account_free(self.quote_asset)
        available_for_risk = max(Decimal("0"), total_equity * (Decimal("1") - self.settings.quote_reserve_fraction))
        deployed_value = sum(
            self._account_total(split_symbol(symbol)[0]) * self.engines[symbol].mark_price()
            for symbol in self.symbols
        )
        remaining_risk_budget = max(Decimal("0"), available_for_risk - deployed_value)
        target_position_value = available_for_risk / Decimal(max(1, self.settings.max_active_positions))

        for row in rankings:
            symbol = row["symbol"]
            if len(held_symbols) >= self.settings.max_active_positions and symbol not in held_symbols:
                continue
            if symbol in held_symbols:
                continue
            if row["action"] != "BUY" or parse_decimal(row["confidence"]) < self.settings.portfolio_min_confidence:
                continue
            notional = min(quote_free, target_position_value, remaining_risk_budget)
            if notional < self.settings.minimum_trade_notional_usdt:
                continue
            confidence = parse_decimal(row["confidence"])
            actions.append(PortfolioAction(symbol=symbol, action="BUY", notional_usdt=notional, confidence=confidence, reason="live_ranked_buy_signal"))
            quote_free -= notional
            remaining_risk_budget -= notional
            held_symbols.add(symbol)

        if actions:
            self.record_event("live_rebalance_intent", {"actions": [action.as_dict() for action in actions]})
        return actions

    def _rebalance_paper(self) -> list[PortfolioAction]:
        actions: list[PortfolioAction] = []
        total_equity = self.total_equity_usdt()
        tier_status = self.milestones.register_equity(total_equity)
        if tier_status.trading_locked:
            self.record_event("portfolio_locked", {"reason": tier_status.reason, "locked_until": tier_status.locked_until.isoformat() if tier_status.locked_until else None})
            return actions

        rankings = self.rank_symbols()
        selected_symbols = {row["symbol"] for row in rankings if row["action"] == "BUY" and row["confidence"] >= decimal_float(self.settings.portfolio_min_confidence)}
        selected_symbols = set(list(selected_symbols)[: self.settings.max_active_positions])

        for symbol in list(self.positions):
            signal = self.engines[symbol].last_signal
            should_sell = signal.action == "SELL" and signal.confidence >= self.settings.portfolio_rotation_sell_confidence
            should_rotate = selected_symbols and symbol not in selected_symbols and signal.action != "BUY"
            if should_sell or should_rotate:
                action = self._paper_sell_all(symbol, "sell_signal" if should_sell else "rotation")
                if action:
                    actions.append(action)

        available_for_risk = max(Decimal("0"), self.total_equity_usdt() * (Decimal("1") - self.settings.quote_reserve_fraction))
        target_position_value = available_for_risk / Decimal(max(1, self.settings.max_active_positions))
        for row in rankings:
            symbol = row["symbol"]
            if len(self.positions) >= self.settings.max_active_positions and symbol not in self.positions:
                continue
            if row["action"] != "BUY" or parse_decimal(row["confidence"]) < self.settings.portfolio_min_confidence:
                continue
            if symbol in self.positions:
                continue
            notional = min(self.quote_balance, target_position_value)
            if notional < self.settings.minimum_trade_notional_usdt:
                continue
            action = self._paper_buy(symbol, notional, parse_decimal(row["confidence"]), "ranked_buy_signal")
            if action:
                actions.append(action)

        return actions

    def _paper_buy(self, symbol: str, notional: Decimal, confidence: Decimal, reason: str) -> PortfolioAction | None:
        price = self.engines[symbol].mark_price()
        if price <= 0 or self.quote_balance < notional:
            return None
        quantity = notional / price
        self.quote_balance -= notional
        self.positions[symbol] = {
            "quantity": str(quantity),
            "entry_price": str(price),
            "entry_time": utc_now().isoformat(),
        }
        action = PortfolioAction(symbol=symbol, action="BUY", notional_usdt=notional, confidence=confidence, reason=reason)
        self.record_event("paper_buy", action.as_dict() | {"price": decimal_float(price), "quantity": decimal_float(quantity)})
        return action

    def _paper_sell_all(self, symbol: str, reason: str) -> PortfolioAction | None:
        position = self.positions.get(symbol)
        if not position:
            return None
        price = self.engines[symbol].mark_price()
        quantity = parse_decimal(position.get("quantity"))
        entry = parse_decimal(position.get("entry_price"))
        if price <= 0 or quantity <= 0:
            return None
        gross = quantity * price
        pnl = (price - entry) * quantity if entry > 0 else Decimal("0")
        self.quote_balance += gross
        del self.positions[symbol]
        confidence = self.engines[symbol].last_signal.confidence
        self.engines[symbol].kelly.record_outcome(pnl)
        action = PortfolioAction(symbol=symbol, action="SELL", notional_usdt=gross, confidence=confidence, reason=reason)
        self.record_event("paper_sell", action.as_dict() | {"price": decimal_float(price), "quantity": decimal_float(quantity), "pnl_usdt": decimal_float(pnl)})
        return action

    def total_equity_usdt(self) -> Decimal:
        if self.settings.live_trading_armed and self.account_balances:
            return self._live_total_equity_usdt()
        total = self.quote_balance
        for symbol, position in self.positions.items():
            engine = self.engines.get(symbol)
            if not engine:
                continue
            total += parse_decimal(position.get("quantity")) * engine.mark_price()
        return total

    def _live_total_equity_usdt(self) -> Decimal:
        total = self._account_total(self.quote_asset)
        for symbol in self.symbols:
            base_asset = split_symbol(symbol)[0]
            mark = self.engines[symbol].mark_price()
            if mark > 0:
                total += self._account_total(base_asset) * mark
        return total

    def _account_free(self, asset: str) -> Decimal:
        return parse_decimal((self.account_balances.get(asset.upper()) or {}).get("free"))

    def _account_locked(self, asset: str) -> Decimal:
        return parse_decimal((self.account_balances.get(asset.upper()) or {}).get("locked"))

    def _account_total(self, asset: str) -> Decimal:
        return self._account_free(asset) + self._account_locked(asset)

    def _live_held_symbols(self) -> set[str]:
        held: set[str] = set()
        for symbol in self.symbols:
            base_asset = split_symbol(symbol)[0]
            quantity = self._account_total(base_asset)
            mark = self.engines[symbol].mark_price()
            if quantity > 0 and quantity * mark >= self.settings.minimum_trade_notional_usdt:
                held.add(symbol)
        return held

    def _is_symbol_held(self, symbol: str) -> bool:
        if self.settings.live_trading_armed and self.account_balances:
            return symbol in self._live_held_symbols()
        return symbol in self.positions

    def rank_symbols(self) -> list[JsonDict]:
        rows: list[JsonDict] = []
        for symbol, engine in self.engines.items():
            signal = engine.last_signal
            research = engine.telemetry().get("research", {})
            backtest = research.get("backtest", {}) if isinstance(research, dict) else {}
            confidence = signal.confidence
            action_bonus = Decimal("0.20") if signal.action == "BUY" else Decimal("-0.10") if signal.action == "SELL" else Decimal("0")
            research_score = parse_decimal(backtest.get("score")) / Decimal("100")
            rank_score = confidence + action_bonus + research_score
            rows.append(
                {
                    "symbol": symbol,
                    "action": signal.action,
                    "confidence": decimal_float(confidence),
                    "rank_score": decimal_float(rank_score),
                    "reason": signal.reason,
                    "mark_price": decimal_float(engine.mark_price()),
                    "obi": (engine.last_book_metrics.as_dict().get("imbalance") if engine.last_book_metrics else None),
                    "z_score": (engine.last_indicators.as_dict().get("z_score") if engine.last_indicators else None),
                    "research_loaded": bool(research.get("profile_loaded")) if isinstance(research, dict) else False,
                    "held": self._is_symbol_held(symbol),
                }
            )
        return sorted(rows, key=lambda row: row["rank_score"], reverse=True)

    def _paper_positions_payload(self) -> list[JsonDict]:
        positions_payload = []
        for symbol, position in sorted(self.positions.items()):
            engine = self.engines[symbol]
            quantity = parse_decimal(position.get("quantity"))
            entry = parse_decimal(position.get("entry_price"))
            mark = engine.mark_price()
            value = quantity * mark
            pnl = (mark - entry) * quantity if entry > 0 else Decimal("0")
            positions_payload.append(
                {
                    "symbol": symbol,
                    "quantity": decimal_float(quantity),
                    "entry_price": decimal_float(entry),
                    "mark_price": decimal_float(mark),
                    "value_usdt": decimal_float(value),
                    "unrealized_pnl_usdt": decimal_float(pnl),
                }
            )
        return positions_payload

    def _live_positions_payload(self) -> list[JsonDict]:
        positions_payload = []
        for symbol in sorted(self.symbols):
            base_asset = split_symbol(symbol)[0]
            quantity = self._account_total(base_asset)
            mark = self.engines[symbol].mark_price()
            value = quantity * mark
            if quantity <= 0 or value < self.settings.minimum_trade_notional_usdt:
                continue
            stored = self.positions.get(symbol, {})
            entry = parse_decimal(stored.get("entry_price"))
            pnl = (mark - entry) * quantity if entry > 0 else Decimal("0")
            positions_payload.append(
                {
                    "symbol": symbol,
                    "quantity": decimal_float(quantity),
                    "entry_price": decimal_float(entry),
                    "mark_price": decimal_float(mark),
                    "value_usdt": decimal_float(value),
                    "unrealized_pnl_usdt": decimal_float(pnl),
                }
            )
        return positions_payload

    def _balance_payload(self) -> JsonDict:
        if self.settings.live_trading_armed and self.account_balances:
            wanted_assets = {self.quote_asset}
            wanted_assets.update(split_symbol(symbol)[0] for symbol in self.symbols)
            return {
                asset: {
                    "free": decimal_float(self._account_free(asset)),
                    "locked": decimal_float(self._account_locked(asset)),
                    "total": decimal_float(self._account_total(asset)),
                }
                for asset in sorted(wanted_assets)
                if self._account_total(asset) != 0 or asset == self.quote_asset
            }
        return {
            self.quote_asset: {
                "free": decimal_float(self.quote_balance),
                "locked": 0.0,
                "total": decimal_float(self.quote_balance),
            }
        }

    def top_symbol(self) -> str:
        rankings = self.rank_symbols()
        return str(rankings[0]["symbol"]) if rankings else self.symbols[0]

    def telemetry(self, websocket_health: JsonDict | None = None) -> JsonDict:
        self.rebalance_if_due()
        equity = self.total_equity_usdt()
        tier_status = self.milestones.register_equity(equity)
        rankings = self.rank_symbols()
        top_symbol = self.top_symbol()
        top_engine = self.engines[top_symbol]
        top_telemetry = top_engine.telemetry(websocket_health)
        live_account = self.settings.live_trading_armed and bool(self.account_balances)
        positions_payload = self._live_positions_payload() if live_account else self._paper_positions_payload()
        quote_free = self._account_free(self.quote_asset) if live_account else self.quote_balance

        top_telemetry["symbol"] = ",".join(self.symbols)
        top_telemetry["system_balance"] = {
            "total_equity_usdt": decimal_float(equity),
            "quote_asset": self.quote_asset,
            "base_asset": "MULTI",
            "balances": self._balance_payload(),
        }
        top_telemetry["tier_progression"] = tier_status.as_dict()
        top_telemetry["portfolio"] = {
            "enabled": True,
            "symbols": list(self.symbols),
            "top_symbol": top_symbol,
            "live_trading_armed": self.settings.live_trading_armed,
            "account_source": "mexc_live_account" if live_account else "paper_state",
            "quote_balance": decimal_float(quote_free),
            "total_equity_usdt": decimal_float(equity),
            "max_active_positions": self.settings.max_active_positions,
            "quote_reserve_fraction": decimal_float(self.settings.quote_reserve_fraction),
            "positions": positions_payload,
            "rankings": rankings,
            "last_actions": [action.as_dict() for action in self.last_actions],
        }
        top_telemetry["symbols"] = {symbol: engine.telemetry(websocket_health) for symbol, engine in self.engines.items()}
        combined_logs = list(self.execution_log)[-60:] + [entry for engine in self.engines.values() for entry in list(engine.execution_log)[-8:]]
        combined_logs = sorted(combined_logs, key=lambda entry: entry.timestamp)[-35:]
        top_telemetry["execution_logs"] = [entry.as_dict() for entry in combined_logs]
        self._persist()
        return top_telemetry

    def _persist(self) -> None:
        self.store.save(
            {
                "quote_balance": str(self.quote_balance),
                "positions": self.positions,
                "milestones": self.milestones.as_state(),
                "execution_log": [entry.as_dict() for entry in list(self.execution_log)[-100:]],
                "updated_at": utc_now().isoformat(),
            }
        )
