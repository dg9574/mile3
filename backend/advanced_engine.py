from __future__ import annotations

import json
import math
import os
import statistics
import tempfile
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from pathlib import Path
from typing import Any

from backend.config import CapitalTier, Settings, split_symbol


JsonDict = dict[str, Any]


def utc_now() -> datetime:
    return datetime.now(tz=UTC)


def parse_decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    if value is None:
        return default
    try:
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return default


def decimal_float(value: Decimal) -> float:
    if value.is_nan() or value.is_infinite():
        return 0.0
    return float(value)


def decimal_str(value: Decimal, places: str = "0.00000001") -> str:
    if value.is_nan() or value.is_infinite():
        return "0"
    quant = Decimal(places)
    return str(value.quantize(quant, rounding=ROUND_DOWN).normalize())


@dataclass(slots=True)
class BookMetrics:
    symbol: str
    best_bid: Decimal
    best_ask: Decimal
    mid_price: Decimal
    spread_bps: Decimal
    bid_liquidity_top5: Decimal
    ask_liquidity_top5: Decimal
    imbalance: Decimal
    update_time_ms: int

    def as_dict(self) -> JsonDict:
        return {
            "symbol": self.symbol,
            "best_bid": decimal_float(self.best_bid),
            "best_ask": decimal_float(self.best_ask),
            "mid_price": decimal_float(self.mid_price),
            "spread_bps": decimal_float(self.spread_bps),
            "bid_liquidity_top5": decimal_float(self.bid_liquidity_top5),
            "ask_liquidity_top5": decimal_float(self.ask_liquidity_top5),
            "imbalance": decimal_float(self.imbalance),
            "update_time_ms": self.update_time_ms,
        }


@dataclass(slots=True)
class OUStats:
    theta: Decimal = Decimal("0")
    mu: Decimal = Decimal("0")
    sigma: Decimal = Decimal("0")
    half_life_periods: Decimal = Decimal("0")
    velocity: Decimal = Decimal("0")

    def as_dict(self) -> JsonDict:
        return {
            "theta": decimal_float(self.theta),
            "mu": decimal_float(self.mu),
            "sigma": decimal_float(self.sigma),
            "half_life_periods": decimal_float(self.half_life_periods),
            "velocity": decimal_float(self.velocity),
        }


@dataclass(slots=True)
class StrategyIndicators:
    rolling_mean: Decimal
    rolling_stddev: Decimal
    upper_band: Decimal
    lower_band: Decimal
    z_score: Decimal
    realized_volatility: Decimal
    ou: OUStats

    def as_dict(self) -> JsonDict:
        return {
            "rolling_mean": decimal_float(self.rolling_mean),
            "rolling_stddev": decimal_float(self.rolling_stddev),
            "upper_band": decimal_float(self.upper_band),
            "lower_band": decimal_float(self.lower_band),
            "z_score": decimal_float(self.z_score),
            "realized_volatility": decimal_float(self.realized_volatility),
            "ou": self.ou.as_dict(),
        }


@dataclass(slots=True)
class MarketSignal:
    action: str
    confidence: Decimal
    reason: str
    recommended_notional_usdt: Decimal
    kelly_fraction: Decimal
    trailing_stop_distance_pct: Decimal
    created_at: datetime = field(default_factory=utc_now)

    def as_dict(self) -> JsonDict:
        return {
            "action": self.action,
            "confidence": decimal_float(self.confidence),
            "reason": self.reason,
            "recommended_notional_usdt": decimal_float(self.recommended_notional_usdt),
            "kelly_fraction": decimal_float(self.kelly_fraction),
            "trailing_stop_distance_pct": decimal_float(self.trailing_stop_distance_pct),
            "created_at": self.created_at.isoformat(),
        }


@dataclass(slots=True)
class TierStatus:
    current_tier: CapitalTier
    next_tier: CapitalTier | None
    equity_usdt: Decimal
    rolling_30d_return_pct: Decimal
    progression_delta_usdt: Decimal
    daily_drawdown_pct: Decimal
    locked_until: datetime | None
    trading_locked: bool
    reason: str

    def as_dict(self) -> JsonDict:
        return {
            "current_tier": self.current_tier.name,
            "current_tier_number": self.current_tier.tier,
            "current_tier_required_equity_usdt": decimal_float(self.current_tier.required_equity_usdt),
            "current_allocation_limit": decimal_float(self.current_tier.allocation_limit),
            "next_tier": self.next_tier.name if self.next_tier else None,
            "next_tier_required_equity_usdt": decimal_float(self.next_tier.required_equity_usdt) if self.next_tier else None,
            "equity_usdt": decimal_float(self.equity_usdt),
            "rolling_30d_return_pct": decimal_float(self.rolling_30d_return_pct),
            "progression_delta_usdt": decimal_float(self.progression_delta_usdt),
            "daily_drawdown_pct": decimal_float(self.daily_drawdown_pct),
            "locked_until": self.locked_until.isoformat() if self.locked_until else None,
            "trading_locked": self.trading_locked,
            "reason": self.reason,
        }


@dataclass(slots=True)
class ExecutionLogEntry:
    timestamp: datetime
    event: str
    details: JsonDict

    def as_dict(self) -> JsonDict:
        return {
            "timestamp": self.timestamp.isoformat(),
            "event": self.event,
            "details": self.details,
        }


class EngineStateStore:
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


class OrderBookImbalanceCalculator:
    def __init__(self, symbol: str) -> None:
        self.symbol = symbol

    def compute(self, update: JsonDict) -> BookMetrics | None:
        depth = update.get("publicLimitDepths") or update.get("public_limit_depths") or update
        asks = self._normalize_levels(depth.get("asks") or [])
        bids = self._normalize_levels(depth.get("bids") or [])
        if not asks or not bids:
            return None

        asks = sorted(asks, key=lambda item: item[0])[:5]
        bids = sorted(bids, key=lambda item: item[0], reverse=True)[:5]
        best_ask = asks[0][0]
        best_bid = bids[0][0]
        if best_bid <= 0 or best_ask <= 0:
            return None

        bid_liquidity = self._weighted_notional(bids)
        ask_liquidity = self._weighted_notional(asks)
        denominator = bid_liquidity + ask_liquidity
        imbalance = Decimal("0") if denominator <= 0 else (bid_liquidity - ask_liquidity) / denominator
        mid = (best_bid + best_ask) / Decimal("2")
        spread_bps = ((best_ask - best_bid) / mid) * Decimal("10000") if mid > 0 else Decimal("0")
        update_time = int(
            parse_decimal(
                update.get("sendTime")
                or update.get("createTime")
                or depth.get("lastOrderCreateTime")
                or int(time.time() * 1000)
            )
        )

        return BookMetrics(
            symbol=str(update.get("symbol") or self.symbol),
            best_bid=best_bid,
            best_ask=best_ask,
            mid_price=mid,
            spread_bps=spread_bps,
            bid_liquidity_top5=bid_liquidity,
            ask_liquidity_top5=ask_liquidity,
            imbalance=imbalance,
            update_time_ms=update_time,
        )

    @staticmethod
    def _normalize_levels(levels: list[Any]) -> list[tuple[Decimal, Decimal]]:
        normalized: list[tuple[Decimal, Decimal]] = []
        for level in levels:
            if isinstance(level, dict):
                price = parse_decimal(level.get("price"))
                quantity = parse_decimal(level.get("quantity"))
            elif isinstance(level, (list, tuple)) and len(level) >= 2:
                price = parse_decimal(level[0])
                quantity = parse_decimal(level[1])
            else:
                continue
            if price > 0 and quantity > 0:
                normalized.append((price, quantity))
        return normalized

    @staticmethod
    def _weighted_notional(levels: list[tuple[Decimal, Decimal]]) -> Decimal:
        total = Decimal("0")
        for index, (price, quantity) in enumerate(levels[:5], start=1):
            weight = Decimal("1") / Decimal(index)
            total += price * quantity * weight
        return total


class RollingMarketMath:
    def __init__(self, window: int, bollinger_stddev: Decimal) -> None:
        self.window = max(20, window)
        self.bollinger_stddev = bollinger_stddev
        self.prices: deque[Decimal] = deque(maxlen=self.window * 4)

    def add_price(self, price: Decimal) -> None:
        if price > 0:
            self.prices.append(price)

    def indicators(self) -> StrategyIndicators:
        prices = list(self.prices)[-self.window :]
        if len(prices) < 2:
            price = prices[-1] if prices else Decimal("0")
            return StrategyIndicators(
                rolling_mean=price,
                rolling_stddev=Decimal("0"),
                upper_band=price,
                lower_band=price,
                z_score=Decimal("0"),
                realized_volatility=Decimal("0"),
                ou=OUStats(mu=price),
            )

        floats = [float(price) for price in prices]
        mean_f = statistics.fmean(floats)
        std_f = statistics.pstdev(floats) if len(floats) > 1 else 0.0
        mean = Decimal(str(mean_f))
        std = Decimal(str(std_f))
        upper = mean + self.bollinger_stddev * std
        lower = mean - self.bollinger_stddev * std
        last = prices[-1]
        z_score = Decimal("0") if std == 0 else (last - mean) / std
        returns = self._log_returns(prices)
        realized_volatility = Decimal(str(statistics.pstdev(returns))) if len(returns) > 2 else Decimal("0")
        ou = self._ornstein_uhlenbeck(prices)
        return StrategyIndicators(
            rolling_mean=mean,
            rolling_stddev=std,
            upper_band=upper,
            lower_band=lower,
            z_score=z_score,
            realized_volatility=realized_volatility,
            ou=ou,
        )

    @staticmethod
    def _log_returns(prices: list[Decimal]) -> list[float]:
        values: list[float] = []
        for previous, current in zip(prices, prices[1:]):
            if previous > 0 and current > 0:
                values.append(math.log(float(current / previous)))
        return values

    @staticmethod
    def _ornstein_uhlenbeck(prices: list[Decimal]) -> OUStats:
        if len(prices) < 8:
            return OUStats(mu=prices[-1] if prices else Decimal("0"))

        x_prev = [float(price) for price in prices[:-1]]
        x_next = [float(price) for price in prices[1:]]
        mean_prev = statistics.fmean(x_prev)
        mean_next = statistics.fmean(x_next)
        variance_prev = sum((x - mean_prev) ** 2 for x in x_prev)
        if variance_prev <= 0:
            velocity = Decimal(str(x_next[-1] - x_prev[-1]))
            return OUStats(mu=Decimal(str(mean_next)), velocity=velocity)

        covariance = sum((a - mean_prev) * (b - mean_next) for a, b in zip(x_prev, x_next))
        beta = covariance / variance_prev
        beta = min(0.999999, max(0.000001, beta))
        alpha = mean_next - beta * mean_prev
        theta = -math.log(beta)
        mu = alpha / (1.0 - beta)
        residuals = [b - (alpha + beta * a) for a, b in zip(x_prev, x_next)]
        residual_std = statistics.pstdev(residuals) if len(residuals) > 1 else 0.0
        sigma = residual_std * math.sqrt((2.0 * theta) / max(1e-12, 1.0 - beta**2))
        half_life = math.log(2.0) / theta if theta > 0 else 0.0
        velocity = x_next[-1] - x_prev[-1]
        return OUStats(
            theta=Decimal(str(theta)),
            mu=Decimal(str(mu)),
            sigma=Decimal(str(sigma)),
            half_life_periods=Decimal(str(half_life)),
            velocity=Decimal(str(velocity)),
        )


class KellyCriterionSizer:
    def __init__(self, settings: Settings, stored_outcomes: list[str] | None = None) -> None:
        self.settings = settings
        self.outcomes: deque[Decimal] = deque(maxlen=250)
        for value in stored_outcomes or []:
            self.outcomes.append(parse_decimal(value))

    def record_outcome(self, pnl_usdt: Decimal) -> None:
        if pnl_usdt != 0:
            self.outcomes.append(pnl_usdt)

    def sizing_fraction(self, *, volatility: Decimal, confidence: Decimal, allocation_limit: Decimal) -> Decimal:
        cap = min(allocation_limit, self.settings.max_capital_utilization_per_position, self.settings.kelly_fraction_cap)
        if cap <= 0:
            return Decimal("0")

        if len(self.outcomes) < 12:
            edge = max(Decimal("0"), confidence - Decimal("0.50"))
            raw = Decimal("0.0200") + edge * Decimal("0.1000")
        else:
            wins = [item for item in self.outcomes if item > 0]
            losses = [abs(item) for item in self.outcomes if item < 0]
            win_rate = Decimal(len(wins)) / Decimal(len(self.outcomes))
            average_win = sum(wins, Decimal("0")) / Decimal(len(wins)) if wins else Decimal("0")
            average_loss = sum(losses, Decimal("0")) / Decimal(len(losses)) if losses else Decimal("0")
            payoff = average_win / average_loss if average_loss > 0 else Decimal("2")
            if payoff <= 0:
                raw = Decimal("0")
            else:
                raw = win_rate - ((Decimal("1") - win_rate) / payoff)
                raw *= Decimal("0.50")

        if volatility > 0:
            volatility_scalar = min(Decimal("1.50"), self.settings.volatility_position_target / volatility)
        else:
            volatility_scalar = Decimal("1")
        return min(cap, max(Decimal("0"), raw * volatility_scalar))

    def as_state(self) -> list[str]:
        return [str(item) for item in self.outcomes]

    def as_dict(self) -> JsonDict:
        wins = [item for item in self.outcomes if item > 0]
        losses = [item for item in self.outcomes if item < 0]
        return {
            "sample_size": len(self.outcomes),
            "win_count": len(wins),
            "loss_count": len(losses),
            "total_realized_pnl_usdt": decimal_float(sum(self.outcomes, Decimal("0"))),
        }


class MilestoneVerifier:
    def __init__(self, settings: Settings, stored: JsonDict | None = None) -> None:
        self.settings = settings
        stored = stored or {}
        self.equity_snapshots: deque[tuple[datetime, Decimal]] = deque(maxlen=20000)
        for item in stored.get("equity_snapshots", []):
            try:
                timestamp = datetime.fromisoformat(item["timestamp"])
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=UTC)
                self.equity_snapshots.append((timestamp, parse_decimal(item["equity_usdt"])))
            except (KeyError, TypeError, ValueError):
                continue

        self.active_tier_number = int(stored.get("active_tier_number") or 1)
        self.day_anchor_date = stored.get("day_anchor_date")
        self.day_anchor_equity = parse_decimal(stored.get("day_anchor_equity_usdt"), settings.paper_starting_equity_usdt)
        lock_value = stored.get("locked_until")
        self.locked_until: datetime | None = None
        if lock_value:
            try:
                self.locked_until = datetime.fromisoformat(lock_value)
                if self.locked_until.tzinfo is None:
                    self.locked_until = self.locked_until.replace(tzinfo=UTC)
            except ValueError:
                self.locked_until = None

    def register_equity(self, equity_usdt: Decimal, now: datetime | None = None) -> TierStatus:
        now = now or utc_now()
        self._prune(now)
        today = now.date().isoformat()
        if self.day_anchor_date != today or self.day_anchor_equity <= 0:
            self.day_anchor_date = today
            self.day_anchor_equity = equity_usdt

        self.equity_snapshots.append((now, equity_usdt))
        daily_drawdown = Decimal("0")
        if self.day_anchor_equity > 0:
            daily_drawdown = max(Decimal("0"), (self.day_anchor_equity - equity_usdt) / self.day_anchor_equity)
        if daily_drawdown >= self.settings.hard_daily_drawdown_limit_pct:
            breach_lock = now + timedelta(hours=24)
            if self.locked_until is None or breach_lock > self.locked_until:
                self.locked_until = breach_lock

        bracket_tier = self.settings.tier_for_equity(equity_usdt)
        if bracket_tier.tier > self.active_tier_number:
            self.active_tier_number = bracket_tier.tier

        rolling_return = self._rolling_30d_return(equity_usdt)
        active = self._tier_by_number(self.active_tier_number)
        next_tier = self.settings.next_tier(active)
        if next_tier and equity_usdt >= next_tier.required_equity_usdt and rolling_return >= active.monthly_progression_threshold:
            self.active_tier_number = next_tier.tier
            active = next_tier
            next_tier = self.settings.next_tier(active)

        progression_delta = Decimal("0") if next_tier is None else max(Decimal("0"), next_tier.required_equity_usdt - equity_usdt)
        locked = bool(self.locked_until and now < self.locked_until)
        if locked:
            reason = "daily_drawdown_lockout"
        elif daily_drawdown >= self.settings.hard_daily_drawdown_limit_pct * Decimal("0.80"):
            reason = "drawdown_warning"
        elif next_tier is None:
            reason = "maximum_tier_active"
        else:
            reason = "active"

        return TierStatus(
            current_tier=active,
            next_tier=next_tier,
            equity_usdt=equity_usdt,
            rolling_30d_return_pct=rolling_return,
            progression_delta_usdt=progression_delta,
            daily_drawdown_pct=daily_drawdown,
            locked_until=self.locked_until,
            trading_locked=locked,
            reason=reason,
        )

    def _rolling_30d_return(self, current_equity: Decimal) -> Decimal:
        if not self.equity_snapshots:
            return Decimal("0")
        first_equity = self.equity_snapshots[0][1]
        if first_equity <= 0:
            return Decimal("0")
        return (current_equity - first_equity) / first_equity

    def _prune(self, now: datetime) -> None:
        cutoff = now - timedelta(days=30)
        while self.equity_snapshots and self.equity_snapshots[0][0] < cutoff:
            self.equity_snapshots.popleft()

    def _tier_by_number(self, tier_number: int) -> CapitalTier:
        for tier in self.settings.capital_tiers:
            if tier.tier == tier_number:
                return tier
        return self.settings.capital_tiers[0]

    def as_state(self) -> JsonDict:
        return {
            "active_tier_number": self.active_tier_number,
            "day_anchor_date": self.day_anchor_date,
            "day_anchor_equity_usdt": str(self.day_anchor_equity),
            "locked_until": self.locked_until.isoformat() if self.locked_until else None,
            "equity_snapshots": [
                {"timestamp": timestamp.isoformat(), "equity_usdt": str(equity)}
                for timestamp, equity in list(self.equity_snapshots)[-5000:]
            ],
        }


class AdvancedTradingEngine:
    def __init__(self, settings: Settings, *, symbol: str | None = None, state_path: str | None = None, starting_quote_balance: Decimal | None = None) -> None:
        self.settings = settings
        self.symbol = (symbol or settings.trading_symbol).strip().upper()
        self.base_asset, self.quote_asset = split_symbol(self.symbol)
        self.store = EngineStateStore(state_path or self.settings.state_path)
        stored = self.store.load()
        self.research_profile = self._load_research_profile()
        profile_params = self.research_profile.get("strategy_parameters", {}) if self.research_profile else {}
        trade_permission = self.research_profile.get("trade_permission", {}) if self.research_profile else {}
        self.research_trading_allowed = bool(trade_permission.get("paper_trading_allowed", False))
        self.strategy_type = str(profile_params.get("strategy_type") or "mean_reversion")
        bollinger_window = int(profile_params.get("bollinger_window") or settings.bollinger_window)
        bollinger_stddev = parse_decimal(profile_params.get("bollinger_stddev"), settings.bollinger_stddev)
        self.strategy_z_entry = parse_decimal(profile_params.get("z_entry"), Decimal("1.20"))
        self.strategy_entry_score_threshold = parse_decimal(profile_params.get("entry_score_threshold"), Decimal("0.40"))
        self.strategy_obi_weight = parse_decimal(profile_params.get("obi_weight"), Decimal("0.80"))
        self.strategy_velocity_weight = parse_decimal(profile_params.get("velocity_weight"), Decimal("0.10"))
        self.strategy_trend_fast_window = int(profile_params.get("trend_fast_window") or 20)
        self.strategy_trend_slow_window = int(profile_params.get("trend_slow_window") or max(80, self.strategy_trend_fast_window + 5))
        self.strategy_stop_loss_pct = parse_decimal(profile_params.get("stop_loss_pct"), Decimal("0.03"))
        self.strategy_take_profit_pct = parse_decimal(profile_params.get("take_profit_pct"), Decimal("0.04"))
        self.obi = OrderBookImbalanceCalculator(self.symbol)
        self.math = RollingMarketMath(bollinger_window, bollinger_stddev)
        self.milestones = MilestoneVerifier(settings, stored.get("milestones"))
        self.kelly = KellyCriterionSizer(settings, stored.get("kelly_outcomes"))
        self.balances: dict[str, dict[str, Decimal]] = {}
        for asset, balance in stored.get("balances", {}).items():
            self.balances[asset] = {
                "free": parse_decimal(balance.get("free")),
                "locked": parse_decimal(balance.get("locked")),
            }
        self.last_book_metrics: BookMetrics | None = None
        self.last_indicators: StrategyIndicators | None = None
        self.last_signal: MarketSignal = MarketSignal(
            action="HOLD",
            confidence=Decimal("0"),
            reason="engine_initializing",
            recommended_notional_usdt=Decimal("0"),
            kelly_fraction=Decimal("0"),
            trailing_stop_distance_pct=Decimal("0"),
        )
        self.execution_log: deque[ExecutionLogEntry] = deque(maxlen=200)
        self.position_entry_price: Decimal | None = parse_decimal(stored.get("position_entry_price")) if stored.get("position_entry_price") else None
        self.started_at = utc_now()
        if not self.balances:
            self._set_paper_balance(starting_quote_balance or settings.paper_starting_equity_usdt)

    def _load_research_profile(self) -> JsonDict:
        path = Path(self.settings.research_profile_file(self.symbol))
        if not path.exists():
            return {}
        try:
            profile = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        if str(profile.get("symbol") or "").upper() != self.symbol:
            return {}
        return profile if isinstance(profile, dict) else {}

    def _set_paper_balance(self, equity: Decimal) -> None:
        self.balances[self.quote_asset] = {"free": equity, "locked": Decimal("0")}
        self.balances.setdefault(self.base_asset, {"free": Decimal("0"), "locked": Decimal("0")})

    def record_event(self, event: str, details: JsonDict) -> None:
        self.execution_log.append(ExecutionLogEntry(timestamp=utc_now(), event=event, details=details))

    def on_order_book(self, update: JsonDict) -> MarketSignal:
        metrics = self.obi.compute(update)
        if metrics is None:
            self.record_event("book_update_ignored", {"reason": "missing_bid_or_ask"})
            return self.last_signal
        self.last_book_metrics = metrics
        self.math.add_price(metrics.mid_price)
        self.last_indicators = self.math.indicators()
        self.last_signal = self._evaluate_signal(metrics, self.last_indicators)
        self._persist()
        return self.last_signal

    def refresh_depth_snapshot(self, snapshot: JsonDict) -> MarketSignal:
        update = {
            "symbol": self.symbol,
            "sendTime": int(time.time() * 1000),
            "asks": snapshot.get("asks", []),
            "bids": snapshot.get("bids", []),
        }
        return self.on_order_book(update)

    def set_last_price(self, price: Decimal) -> None:
        if price <= 0:
            return
        self.math.add_price(price)
        if self.last_book_metrics is None:
            self.last_book_metrics = BookMetrics(
                symbol=self.symbol,
                best_bid=price,
                best_ask=price,
                mid_price=price,
                spread_bps=Decimal("0"),
                bid_liquidity_top5=Decimal("0"),
                ask_liquidity_top5=Decimal("0"),
                imbalance=Decimal("0"),
                update_time_ms=int(time.time() * 1000),
            )
        self.last_indicators = self.math.indicators()

    def refresh_account(self, account_payload: JsonDict) -> None:
        balances = account_payload.get("balances", [])
        if not isinstance(balances, list):
            return
        for balance in balances:
            asset = str(balance.get("asset") or balance.get("currency") or "").upper()
            if not asset:
                continue
            free = parse_decimal(balance.get("free") or balance.get("available") or balance.get("balanceAmount"))
            locked = parse_decimal(balance.get("locked") or balance.get("frozen") or balance.get("frozenAmount"))
            self.balances[asset] = {"free": free, "locked": locked}
        self.record_event("account_refreshed", {"asset_count": len(self.balances)})
        self._persist()

    def on_private_update(self, message: JsonDict) -> None:
        if "privateAccount" in message:
            self._handle_private_account(message["privateAccount"])
        if "privateOrders" in message:
            self._handle_private_order(message)
        if "privateDeals" in message:
            self._handle_private_deal(message)
        self._persist()

    def _handle_private_account(self, account: JsonDict) -> None:
        asset = str(account.get("vcoinName") or "").upper()
        if not asset:
            return
        self.balances[asset] = {
            "free": parse_decimal(account.get("balanceAmount")),
            "locked": parse_decimal(account.get("frozenAmount")),
        }
        self.record_event("private_account_update", {"asset": asset, "type": account.get("type")})

    def _handle_private_order(self, message: JsonDict) -> None:
        order = message.get("privateOrders") or {}
        self.record_event(
            "private_order_update",
            {
                "symbol": message.get("symbol"),
                "client_id": order.get("clientId"),
                "status": order.get("status"),
                "trade_type": order.get("tradeType"),
                "cumulative_amount": order.get("cumulativeAmount"),
            },
        )

    def _handle_private_deal(self, message: JsonDict) -> None:
        deal = message.get("privateDeals") or {}
        price = parse_decimal(deal.get("price"))
        quantity = parse_decimal(deal.get("quantity"))
        trade_type = int(deal.get("tradeType") or 0)
        amount = parse_decimal(deal.get("amount"))
        if trade_type == 1 and price > 0 and quantity > 0:
            current_qty = self.position_quantity()
            old_entry = self.position_entry_price or price
            new_qty = current_qty + quantity
            if new_qty > 0:
                self.position_entry_price = ((old_entry * current_qty) + (price * quantity)) / new_qty
        elif trade_type == 2 and price > 0 and quantity > 0 and self.position_entry_price:
            realized = (price - self.position_entry_price) * quantity
            self.kelly.record_outcome(realized)
            if self.position_quantity() - quantity <= 0:
                self.position_entry_price = None
        self.record_event(
            "private_deal_update",
            {
                "symbol": message.get("symbol"),
                "trade_type": trade_type,
                "price": decimal_float(price),
                "quantity": decimal_float(quantity),
                "amount": decimal_float(amount),
            },
        )

    def total_equity_usdt(self) -> Decimal:
        quote = self.asset_total(self.quote_asset)
        base = self.asset_total(self.base_asset)
        mark = self.mark_price()
        return quote + (base * mark)

    def asset_total(self, asset: str) -> Decimal:
        balance = self.balances.get(asset.upper(), {})
        return parse_decimal(balance.get("free")) + parse_decimal(balance.get("locked"))

    def position_quantity(self) -> Decimal:
        return self.asset_total(self.base_asset)

    def mark_price(self) -> Decimal:
        if self.last_book_metrics:
            return self.last_book_metrics.mid_price
        if self.math.prices:
            return self.math.prices[-1]
        return Decimal("0")

    def _evaluate_signal(self, metrics: BookMetrics, indicators: StrategyIndicators) -> MarketSignal:
        equity = self.total_equity_usdt()
        tier_status = self.milestones.register_equity(equity)
        active_tier = tier_status.current_tier
        allocation_limit = self.settings.effective_allocation_limit(active_tier)
        confidence = Decimal("0.50")
        action = "HOLD"
        reason_parts: list[str] = []

        if self.strategy_type == "no_trade" or not self.research_trading_allowed:
            trailing_factor = self.settings.trailing_stop_variance_factor(active_tier, indicators.realized_volatility)
            return MarketSignal(
                action="HOLD",
                confidence=Decimal("1"),
                reason="research gate is closed: no calibrated positive edge for this symbol",
                recommended_notional_usdt=Decimal("0"),
                kelly_fraction=Decimal("0"),
                trailing_stop_distance_pct=max(Decimal("0.0025"), indicators.realized_volatility * trailing_factor),
            )

        if self.strategy_type == "trend_following":
            return self._evaluate_trend_following_signal(metrics, indicators, tier_status.current_tier)

        z = indicators.z_score
        obi = metrics.imbalance
        velocity = indicators.ou.velocity
        std = indicators.rolling_stddev
        velocity_scaled = Decimal("0") if std == 0 else velocity / std

        score_span = max(Decimal("0.50"), self.strategy_z_entry * Decimal("2.20"))
        buy_score = max(Decimal("0"), (-z - self.strategy_z_entry) / score_span)
        buy_score += max(Decimal("0"), obi) * self.strategy_obi_weight
        buy_score += max(Decimal("0"), -velocity_scaled) * self.strategy_velocity_weight

        sell_score = max(Decimal("0"), (z - self.strategy_z_entry) / score_span)
        sell_score += max(Decimal("0"), -obi) * self.strategy_obi_weight
        sell_score += max(Decimal("0"), velocity_scaled) * self.strategy_velocity_weight

        if tier_status.trading_locked:
            action = "LOCKED"
            confidence = Decimal("1")
            reason_parts.append("24h lockout is active after daily drawdown breach")
        elif buy_score > sell_score and buy_score >= self.strategy_entry_score_threshold:
            action = "BUY"
            confidence = min(Decimal("0.99"), Decimal("0.50") + buy_score / Decimal("2"))
            reason_parts.append("mean reversion buy: price below dynamic band with supportive liquidity imbalance")
        elif sell_score > buy_score and sell_score >= self.strategy_entry_score_threshold:
            action = "SELL"
            confidence = min(Decimal("0.99"), Decimal("0.50") + sell_score / Decimal("2"))
            reason_parts.append("mean reversion sell: price above dynamic band with weakening bid-side liquidity")
        else:
            reason_parts.append("no statistically dominant mean-reversion edge")

        if confidence < self.settings.min_signal_confidence and action not in {"LOCKED", "HOLD"}:
            action = "HOLD"
            reason_parts.append("confidence below execution threshold")

        kelly_fraction = self.kelly.sizing_fraction(
            volatility=indicators.realized_volatility,
            confidence=confidence,
            allocation_limit=allocation_limit,
        )
        recommended_notional = Decimal("0") if action in {"HOLD", "LOCKED"} else equity * kelly_fraction
        if recommended_notional < self.settings.minimum_trade_notional_usdt:
            recommended_notional = Decimal("0")
            if action not in {"HOLD", "LOCKED"}:
                reason_parts.append("recommended notional below exchange minimum guardrail")

        trailing_factor = self.settings.trailing_stop_variance_factor(active_tier, indicators.realized_volatility)
        trailing_distance = max(Decimal("0.0025"), indicators.realized_volatility * trailing_factor)

        return MarketSignal(
            action=action,
            confidence=confidence,
            reason="; ".join(reason_parts),
            recommended_notional_usdt=recommended_notional,
            kelly_fraction=kelly_fraction,
            trailing_stop_distance_pct=trailing_distance,
        )

    def _evaluate_trend_following_signal(
        self,
        metrics: BookMetrics,
        indicators: StrategyIndicators,
        active_tier: CapitalTier,
    ) -> MarketSignal:
        prices = list(self.math.prices)
        slow_window = max(self.strategy_trend_slow_window, self.strategy_trend_fast_window + 5)
        if len(prices) < slow_window + 2:
            trailing_factor = self.settings.trailing_stop_variance_factor(active_tier, indicators.realized_volatility)
            return MarketSignal(
                action="HOLD",
                confidence=Decimal("0.50"),
                reason=f"trend_following warming up: {len(prices)}/{slow_window + 2} prices available",
                recommended_notional_usdt=Decimal("0"),
                kelly_fraction=Decimal("0"),
                trailing_stop_distance_pct=max(Decimal("0.0025"), indicators.realized_volatility * trailing_factor),
            )

        fast_series = self._ema(prices, self.strategy_trend_fast_window)
        slow_series = self._ema(prices, slow_window)
        fast_now = fast_series[-1]
        fast_prev = fast_series[-2]
        slow_now = slow_series[-1]
        slow_prev = slow_series[-2]
        price = metrics.mid_price
        recent_high = max(prices[-min(24, len(prices)) :])
        fast_slope = (fast_now - fast_prev) / fast_prev if fast_prev > 0 else Decimal("0")
        slow_slope = (slow_now - slow_prev) / slow_prev if slow_prev > 0 else Decimal("0")
        trend_strength = (fast_now - slow_now) / slow_now if slow_now > 0 else Decimal("0")
        breakout = price >= recent_high
        crossover_up = fast_now > slow_now and fast_prev <= slow_prev
        crossover_down = fast_now < slow_now

        trailing_factor = self.settings.trailing_stop_variance_factor(active_tier, indicators.realized_volatility)
        trailing_distance = max(Decimal("0.0025"), indicators.realized_volatility * trailing_factor)
        equity = self.total_equity_usdt()
        allocation_limit = self.settings.effective_allocation_limit(active_tier)

        if crossover_down or trend_strength < Decimal("-0.0015"):
            confidence = min(Decimal("0.98"), Decimal("0.66") + min(Decimal("0.25"), abs(trend_strength) * Decimal("18")))
            return MarketSignal(
                action="SELL",
                confidence=confidence,
                reason="trend_following sell: fast EMA fell below slow EMA or trend strength turned negative",
                recommended_notional_usdt=equity * self.kelly.sizing_fraction(
                    volatility=indicators.realized_volatility,
                    confidence=confidence,
                    allocation_limit=allocation_limit,
                ),
                kelly_fraction=self.kelly.sizing_fraction(
                    volatility=indicators.realized_volatility,
                    confidence=confidence,
                    allocation_limit=allocation_limit,
                ),
                trailing_stop_distance_pct=trailing_distance,
            )

        buy_score = Decimal("0")
        if fast_now > slow_now:
            buy_score += min(Decimal("0.45"), max(Decimal("0"), trend_strength * Decimal("35")))
        if crossover_up:
            buy_score += Decimal("0.25")
        if breakout:
            buy_score += Decimal("0.15")
        if fast_slope > 0 and slow_slope >= 0:
            buy_score += Decimal("0.10")
        if metrics.imbalance > 0:
            buy_score += min(Decimal("0.10"), metrics.imbalance * Decimal("0.20"))

        if buy_score >= Decimal("0.32"):
            confidence = min(Decimal("0.97"), Decimal("0.55") + buy_score)
            kelly_fraction = self.kelly.sizing_fraction(
                volatility=indicators.realized_volatility,
                confidence=confidence,
                allocation_limit=allocation_limit,
            )
            notional = equity * kelly_fraction
            if notional < self.settings.minimum_trade_notional_usdt:
                notional = Decimal("0")
            return MarketSignal(
                action="BUY",
                confidence=confidence,
                reason="trend_following buy: fast EMA above slow EMA with breakout or positive slope confirmation",
                recommended_notional_usdt=notional,
                kelly_fraction=kelly_fraction,
                trailing_stop_distance_pct=trailing_distance,
            )

        return MarketSignal(
            action="HOLD",
            confidence=Decimal("0.55"),
            reason="trend_following hold: trend is not strong enough for a new allocation",
            recommended_notional_usdt=Decimal("0"),
            kelly_fraction=Decimal("0"),
            trailing_stop_distance_pct=trailing_distance,
        )

    @staticmethod
    def _ema(values: list[Decimal], window: int) -> list[Decimal]:
        if not values:
            return []
        alpha = Decimal("2") / Decimal(window + 1)
        output: list[Decimal] = [values[0]]
        for value in values[1:]:
            output.append((value * alpha) + (output[-1] * (Decimal("1") - alpha)))
        return output

    def telemetry(self, websocket_health: JsonDict | None = None) -> JsonDict:
        equity = self.total_equity_usdt()
        tier_status = self.milestones.register_equity(equity)
        book = self.last_book_metrics.as_dict() if self.last_book_metrics else None
        indicators = self.last_indicators.as_dict() if self.last_indicators else None
        position_qty = self.position_quantity()
        mark = self.mark_price()
        entry = self.position_entry_price or Decimal("0")
        unrealized = Decimal("0")
        if position_qty > 0 and entry > 0 and mark > 0:
            unrealized = (mark - entry) * position_qty

        payload = {
            "app": self.settings.app_name,
            "symbol": self.symbol,
            "mode": self.settings.trading_mode,
            "started_at": self.started_at.isoformat(),
            "timestamp": utc_now().isoformat(),
            "system_balance": {
                "total_equity_usdt": decimal_float(equity),
                "quote_asset": self.quote_asset,
                "base_asset": self.base_asset,
                "balances": {
                    asset: {
                        "free": decimal_float(values.get("free", Decimal("0"))),
                        "locked": decimal_float(values.get("locked", Decimal("0"))),
                        "total": decimal_float(values.get("free", Decimal("0")) + values.get("locked", Decimal("0"))),
                    }
                    for asset, values in sorted(self.balances.items())
                },
            },
            "tier_progression": tier_status.as_dict(),
            "active_trade_metrics": {
                "position_base_quantity": decimal_float(position_qty),
                "entry_price": decimal_float(entry),
                "mark_price": decimal_float(mark),
                "unrealized_pnl_usdt": decimal_float(unrealized),
                "last_signal": self.last_signal.as_dict(),
                "kelly": self.kelly.as_dict(),
            },
            "market": {
                "order_book": book,
                "indicators": indicators,
            },
            "risk": {
                "hard_daily_drawdown_limit_pct": decimal_float(self.settings.hard_daily_drawdown_limit_pct),
                "max_capital_utilization_per_position": decimal_float(self.settings.max_capital_utilization_per_position),
                "trailing_stop_variance_factors": {
                    "floor": decimal_float(self.settings.trailing_stop_floor_variance_factor),
                    "base": decimal_float(self.settings.trailing_stop_base_variance_factor),
                    "ceiling": decimal_float(self.settings.trailing_stop_ceiling_variance_factor),
                },
            },
            "research": self._research_telemetry(),
            "websocket": websocket_health or {},
            "execution_logs": [entry.as_dict() for entry in list(self.execution_log)[-25:]],
        }
        self._persist()
        return payload

    def _research_telemetry(self) -> JsonDict:
        if not self.research_profile:
            return {
                "profile_loaded": False,
                "profile_path": self.settings.research_profile_file(self.symbol),
                "status": "not_calibrated",
            }
        backtest = self.research_profile.get("backtest") or {}
        equity_curve = backtest.get("equity_curve") or []
        return {
            "profile_loaded": True,
            "profile_path": self.settings.research_profile_file(self.symbol),
            "generated_at": self.research_profile.get("generated_at"),
            "interval": self.research_profile.get("interval"),
            "lookback_days": self.research_profile.get("lookback_days"),
            "candle_count": self.research_profile.get("candle_count"),
            "strategy_parameters": self.research_profile.get("strategy_parameters") or {},
            "trade_permission": self.research_profile.get("trade_permission") or {},
            "backtest": {
                "total_return_pct": backtest.get("total_return_pct"),
                "max_drawdown_pct": backtest.get("max_drawdown_pct"),
                "sharpe": backtest.get("sharpe"),
                "win_rate": backtest.get("win_rate"),
                "profit_factor": backtest.get("profit_factor"),
                "trade_count": backtest.get("trade_count"),
                "score": backtest.get("score"),
                "equity_curve": equity_curve[-500:] if isinstance(equity_curve, list) else [],
            },
            "limitations": self.research_profile.get("limitations") or [],
        }

    def _persist(self) -> None:
        state = {
            "balances": {
                asset: {"free": str(values.get("free", Decimal("0"))), "locked": str(values.get("locked", Decimal("0")))}
                for asset, values in self.balances.items()
            },
            "milestones": self.milestones.as_state(),
            "kelly_outcomes": self.kelly.as_state(),
            "position_entry_price": str(self.position_entry_price) if self.position_entry_price else None,
            "updated_at": utc_now().isoformat(),
        }
        self.store.save(state)
