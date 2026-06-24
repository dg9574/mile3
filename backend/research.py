from __future__ import annotations

import asyncio
import json
import math
import os
import statistics
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from backend.advanced_engine import decimal_float, parse_decimal
from backend.config import Settings
from backend.mexc_pro import MexcRESTClient


JsonDict = dict[str, Any]

INTERVAL_MS: dict[str, int] = {
    "1m": 60_000,
    "5m": 5 * 60_000,
    "15m": 15 * 60_000,
    "30m": 30 * 60_000,
    "60m": 60 * 60_000,
    "4h": 4 * 60 * 60_000,
    "1d": 24 * 60 * 60_000,
    "1W": 7 * 24 * 60 * 60_000,
    "1M": 30 * 24 * 60 * 60_000,
}


@dataclass(slots=True)
class Candle:
    open_time_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    close_time_ms: int
    quote_volume: Decimal
    trade_count: int

    @classmethod
    def from_mexc(cls, row: list[Any]) -> "Candle":
        if len(row) < 6:
            raise ValueError("MEXC kline row must contain at least six values")
        return cls(
            open_time_ms=int(row[0]),
            open=parse_decimal(row[1]),
            high=parse_decimal(row[2]),
            low=parse_decimal(row[3]),
            close=parse_decimal(row[4]),
            volume=parse_decimal(row[5]),
            close_time_ms=int(row[6]) if len(row) > 6 and row[6] is not None else int(row[0]),
            quote_volume=parse_decimal(row[7]) if len(row) > 7 else Decimal("0"),
            trade_count=int(row[8]) if len(row) > 8 and row[8] is not None else 0,
        )

    def as_dict(self) -> JsonDict:
        return {
            "open_time_ms": self.open_time_ms,
            "open": decimal_float(self.open),
            "high": decimal_float(self.high),
            "low": decimal_float(self.low),
            "close": decimal_float(self.close),
            "volume": decimal_float(self.volume),
            "close_time_ms": self.close_time_ms,
            "quote_volume": decimal_float(self.quote_volume),
            "trade_count": self.trade_count,
        }


@dataclass(frozen=True, slots=True)
class StrategyParameters:
    bollinger_window: int
    bollinger_stddev: Decimal
    z_entry: Decimal
    z_exit: Decimal
    stop_loss_pct: Decimal
    take_profit_pct: Decimal
    max_hold_bars: int
    allocation_fraction: Decimal
    fee_rate: Decimal
    strategy_type: str = "mean_reversion"
    trend_fast_window: int = 20
    trend_slow_window: int = 80

    def as_dict(self) -> JsonDict:
        return {
            "strategy_type": self.strategy_type,
            "bollinger_window": self.bollinger_window,
            "bollinger_stddev": decimal_float(self.bollinger_stddev),
            "z_entry": decimal_float(self.z_entry),
            "z_exit": decimal_float(self.z_exit),
            "stop_loss_pct": decimal_float(self.stop_loss_pct),
            "take_profit_pct": decimal_float(self.take_profit_pct),
            "max_hold_bars": self.max_hold_bars,
            "allocation_fraction": decimal_float(self.allocation_fraction),
            "fee_rate": decimal_float(self.fee_rate),
            "trend_fast_window": self.trend_fast_window,
            "trend_slow_window": self.trend_slow_window,
            "entry_score_threshold": 0.36,
            "obi_weight": 0.80,
            "velocity_weight": 0.10,
        }


@dataclass(slots=True)
class BacktestTrade:
    side: str
    entry_time_ms: int
    exit_time_ms: int
    entry_price: Decimal
    exit_price: Decimal
    quantity: Decimal
    pnl_usdt: Decimal
    return_pct: Decimal
    reason: str

    def as_dict(self) -> JsonDict:
        return {
            "side": self.side,
            "entry_time_ms": self.entry_time_ms,
            "exit_time_ms": self.exit_time_ms,
            "entry_price": decimal_float(self.entry_price),
            "exit_price": decimal_float(self.exit_price),
            "quantity": decimal_float(self.quantity),
            "pnl_usdt": decimal_float(self.pnl_usdt),
            "return_pct": decimal_float(self.return_pct),
            "reason": self.reason,
        }


@dataclass(slots=True)
class BacktestResult:
    parameters: StrategyParameters
    final_equity_usdt: Decimal
    total_return_pct: Decimal
    max_drawdown_pct: Decimal
    sharpe: Decimal
    win_rate: Decimal
    profit_factor: Decimal
    trade_count: int
    score: Decimal
    equity_curve: list[Decimal]
    trades: list[BacktestTrade]

    def as_dict(self, include_curve: bool = False, include_trades: bool = False) -> JsonDict:
        payload: JsonDict = {
            "parameters": self.parameters.as_dict(),
            "final_equity_usdt": decimal_float(self.final_equity_usdt),
            "total_return_pct": decimal_float(self.total_return_pct),
            "max_drawdown_pct": decimal_float(self.max_drawdown_pct),
            "sharpe": decimal_float(self.sharpe),
            "win_rate": decimal_float(self.win_rate),
            "profit_factor": decimal_float(self.profit_factor),
            "trade_count": self.trade_count,
            "score": decimal_float(self.score),
        }
        if include_curve:
            payload["equity_curve"] = [decimal_float(point) for point in self.equity_curve]
        if include_trades:
            payload["trades"] = [trade.as_dict() for trade in self.trades[-100:]]
        return payload


class HistoricalDataCollector:
    def __init__(self, rest_client: MexcRESTClient) -> None:
        self.rest_client = rest_client

    async def fetch_candles(self, symbol: str, interval: str, days: int) -> list[Candle]:
        if interval not in INTERVAL_MS:
            raise ValueError(f"Unsupported interval {interval}; choose one of {', '.join(INTERVAL_MS)}")
        bounded_days = max(1, min(365, int(days)))
        interval_ms = INTERVAL_MS[interval]
        end_time = int(datetime.now(tz=UTC).timestamp() * 1000)
        start_time = int((datetime.now(tz=UTC) - timedelta(days=bounded_days)).timestamp() * 1000)
        cursor = start_time
        by_open_time: dict[int, Candle] = {}

        while cursor < end_time:
            batch = await self.rest_client.klines(
                symbol=symbol,
                interval=interval,
                start_time=cursor,
                end_time=end_time,
                limit=500,
            )
            if not batch:
                break
            candles = []
            for row in batch:
                try:
                    candle = Candle.from_mexc(row)
                except (ValueError, TypeError):
                    continue
                if candle.close > 0:
                    candles.append(candle)
                    by_open_time[candle.open_time_ms] = candle
            if not candles:
                break
            next_cursor = max(candle.open_time_ms for candle in candles) + interval_ms
            if next_cursor <= cursor:
                break
            cursor = next_cursor
            await asyncio.sleep(0.05)

        return [by_open_time[key] for key in sorted(by_open_time)]


class MeanReversionBacktester:
    def __init__(self, starting_equity_usdt: Decimal = Decimal("100")) -> None:
        self.starting_equity = starting_equity_usdt

    def run(self, candles: list[Candle], params: StrategyParameters, interval: str) -> BacktestResult:
        if params.strategy_type == "no_trade":
            return self._no_trade_result(params, candles)
        if params.strategy_type == "trend_following":
            return self._run_trend_following(candles, params, interval)
        if len(candles) < params.bollinger_window + 20:
            return self._empty_result(params)

        cash = self.starting_equity
        quantity = Decimal("0")
        entry_price = Decimal("0")
        entry_time = 0
        entry_cost = Decimal("0")
        hold_bars = 0
        trades: list[BacktestTrade] = []
        equity_curve: list[Decimal] = []

        closes = [candle.close for candle in candles]
        for index in range(params.bollinger_window, len(candles)):
            candle = candles[index]
            window = closes[index - params.bollinger_window : index]
            mean, std = self._mean_std(window)
            z_score = Decimal("0") if std <= 0 else (candle.close - mean) / std

            mark_equity = cash + quantity * candle.close
            equity_curve.append(mark_equity)
            if quantity > 0:
                hold_bars += 1
                change_pct = (candle.close - entry_price) / entry_price if entry_price > 0 else Decimal("0")
                exit_reason = ""
                if change_pct <= -params.stop_loss_pct:
                    exit_reason = "stop_loss"
                elif change_pct >= params.take_profit_pct:
                    exit_reason = "take_profit"
                elif z_score >= -params.z_exit:
                    exit_reason = "mean_reversion_exit"
                elif hold_bars >= params.max_hold_bars:
                    exit_reason = "max_hold"

                if exit_reason:
                    gross = quantity * candle.close
                    fee = gross * params.fee_rate
                    proceeds = gross - fee
                    cash += proceeds
                    pnl = proceeds - entry_cost
                    trades.append(
                        BacktestTrade(
                            side="LONG",
                            entry_time_ms=entry_time,
                            exit_time_ms=candle.close_time_ms,
                            entry_price=entry_price,
                            exit_price=candle.close,
                            quantity=quantity,
                            pnl_usdt=pnl,
                            return_pct=pnl / entry_cost if entry_cost > 0 else Decimal("0"),
                            reason=exit_reason,
                        )
                    )
                    quantity = Decimal("0")
                    entry_price = Decimal("0")
                    entry_time = 0
                    entry_cost = Decimal("0")
                    hold_bars = 0
                    equity_curve[-1] = cash
                    continue

            if quantity == 0 and z_score <= -params.z_entry and cash > Decimal("0"):
                spend = cash * params.allocation_fraction
                if spend > Decimal("5"):
                    fee = spend * params.fee_rate
                    net_spend = spend - fee
                    quantity = net_spend / candle.close
                    cash -= spend
                    entry_price = candle.close
                    entry_time = candle.open_time_ms
                    entry_cost = spend
                    hold_bars = 0
                    equity_curve[-1] = cash + quantity * candle.close

        if quantity > 0:
            final = candles[-1]
            gross = quantity * final.close
            proceeds = gross - gross * params.fee_rate
            cash += proceeds
            pnl = proceeds - entry_cost
            trades.append(
                BacktestTrade(
                    side="LONG",
                    entry_time_ms=entry_time,
                    exit_time_ms=final.close_time_ms,
                    entry_price=entry_price,
                    exit_price=final.close,
                    quantity=quantity,
                    pnl_usdt=pnl,
                    return_pct=pnl / entry_cost if entry_cost > 0 else Decimal("0"),
                    reason="final_liquidation",
                )
            )
            equity_curve.append(cash)

        final_equity = cash
        total_return = (final_equity - self.starting_equity) / self.starting_equity if self.starting_equity > 0 else Decimal("0")
        max_drawdown = self._max_drawdown(equity_curve)
        sharpe = self._sharpe(equity_curve, interval)
        wins = [trade.pnl_usdt for trade in trades if trade.pnl_usdt > 0]
        losses = [trade.pnl_usdt for trade in trades if trade.pnl_usdt < 0]
        win_rate = Decimal(len(wins)) / Decimal(len(trades)) if trades else Decimal("0")
        profit_factor = sum(wins, Decimal("0")) / abs(sum(losses, Decimal("0"))) if losses else (Decimal("4") if wins else Decimal("0"))
        score = self._score(total_return, max_drawdown, sharpe, win_rate, profit_factor, len(trades))
        return BacktestResult(
            parameters=params,
            final_equity_usdt=final_equity,
            total_return_pct=total_return,
            max_drawdown_pct=max_drawdown,
            sharpe=sharpe,
            win_rate=win_rate,
            profit_factor=profit_factor,
            trade_count=len(trades),
            score=score,
            equity_curve=equity_curve,
            trades=trades,
        )

    def _run_trend_following(self, candles: list[Candle], params: StrategyParameters, interval: str) -> BacktestResult:
        slow = max(params.trend_slow_window, params.trend_fast_window + 5)
        if len(candles) < slow + 20:
            return self._empty_result(params)

        cash = self.starting_equity
        quantity = Decimal("0")
        entry_price = Decimal("0")
        entry_time = 0
        entry_cost = Decimal("0")
        hold_bars = 0
        trades: list[BacktestTrade] = []
        equity_curve: list[Decimal] = []
        closes = [candle.close for candle in candles]
        fast_ema = self._ema(closes, params.trend_fast_window)
        slow_ema = self._ema(closes, slow)

        for index in range(slow + 1, len(candles)):
            candle = candles[index]
            mark_equity = cash + quantity * candle.close
            equity_curve.append(mark_equity)

            trend_up = fast_ema[index] > slow_ema[index] and fast_ema[index - 1] <= slow_ema[index - 1]
            trend_down = fast_ema[index] < slow_ema[index]
            recent_high = max(closes[index - min(24, index) : index])
            breakout = candle.close >= recent_high

            if quantity > 0:
                hold_bars += 1
                change_pct = (candle.close - entry_price) / entry_price if entry_price > 0 else Decimal("0")
                exit_reason = ""
                if change_pct <= -params.stop_loss_pct:
                    exit_reason = "stop_loss"
                elif change_pct >= params.take_profit_pct:
                    exit_reason = "take_profit"
                elif trend_down:
                    exit_reason = "trend_reversal"
                elif hold_bars >= params.max_hold_bars:
                    exit_reason = "max_hold"

                if exit_reason:
                    gross = quantity * candle.close
                    fee = gross * params.fee_rate
                    proceeds = gross - fee
                    cash += proceeds
                    pnl = proceeds - entry_cost
                    trades.append(
                        BacktestTrade(
                            side="LONG",
                            entry_time_ms=entry_time,
                            exit_time_ms=candle.close_time_ms,
                            entry_price=entry_price,
                            exit_price=candle.close,
                            quantity=quantity,
                            pnl_usdt=pnl,
                            return_pct=pnl / entry_cost if entry_cost > 0 else Decimal("0"),
                            reason=exit_reason,
                        )
                    )
                    quantity = Decimal("0")
                    entry_price = Decimal("0")
                    entry_time = 0
                    entry_cost = Decimal("0")
                    hold_bars = 0
                    equity_curve[-1] = cash
                    continue

            if quantity == 0 and (trend_up or (fast_ema[index] > slow_ema[index] and breakout)) and cash > Decimal("0"):
                spend = cash * params.allocation_fraction
                if spend > Decimal("5"):
                    fee = spend * params.fee_rate
                    net_spend = spend - fee
                    quantity = net_spend / candle.close
                    cash -= spend
                    entry_price = candle.close
                    entry_time = candle.open_time_ms
                    entry_cost = spend
                    hold_bars = 0
                    equity_curve[-1] = cash + quantity * candle.close

        if quantity > 0:
            final = candles[-1]
            gross = quantity * final.close
            proceeds = gross - gross * params.fee_rate
            cash += proceeds
            pnl = proceeds - entry_cost
            trades.append(
                BacktestTrade(
                    side="LONG",
                    entry_time_ms=entry_time,
                    exit_time_ms=final.close_time_ms,
                    entry_price=entry_price,
                    exit_price=final.close,
                    quantity=quantity,
                    pnl_usdt=pnl,
                    return_pct=pnl / entry_cost if entry_cost > 0 else Decimal("0"),
                    reason="final_liquidation",
                )
            )
            equity_curve.append(cash)

        return self._result_from_equity(params, cash, equity_curve, trades, interval)

    def _result_from_equity(
        self,
        params: StrategyParameters,
        final_equity: Decimal,
        equity_curve: list[Decimal],
        trades: list[BacktestTrade],
        interval: str,
    ) -> BacktestResult:
        total_return = (final_equity - self.starting_equity) / self.starting_equity if self.starting_equity > 0 else Decimal("0")
        max_drawdown = self._max_drawdown(equity_curve)
        sharpe = self._sharpe(equity_curve, interval)
        wins = [trade.pnl_usdt for trade in trades if trade.pnl_usdt > 0]
        losses = [trade.pnl_usdt for trade in trades if trade.pnl_usdt < 0]
        win_rate = Decimal(len(wins)) / Decimal(len(trades)) if trades else Decimal("0")
        profit_factor = sum(wins, Decimal("0")) / abs(sum(losses, Decimal("0"))) if losses else (Decimal("4") if wins else Decimal("0"))
        score = self._score(total_return, max_drawdown, sharpe, win_rate, profit_factor, len(trades))
        return BacktestResult(
            parameters=params,
            final_equity_usdt=final_equity,
            total_return_pct=total_return,
            max_drawdown_pct=max_drawdown,
            sharpe=sharpe,
            win_rate=win_rate,
            profit_factor=profit_factor,
            trade_count=len(trades),
            score=score,
            equity_curve=equity_curve or [self.starting_equity],
            trades=trades,
        )

    def _no_trade_result(self, params: StrategyParameters, candles: list[Candle]) -> BacktestResult:
        curve = [self.starting_equity for _ in candles[-min(500, len(candles)) :]] or [self.starting_equity]
        return BacktestResult(
            parameters=params,
            final_equity_usdt=self.starting_equity,
            total_return_pct=Decimal("0"),
            max_drawdown_pct=Decimal("0"),
            sharpe=Decimal("0"),
            win_rate=Decimal("0"),
            profit_factor=Decimal("0"),
            trade_count=0,
            score=Decimal("0"),
            equity_curve=curve,
            trades=[],
        )

    def _empty_result(self, params: StrategyParameters) -> BacktestResult:
        return BacktestResult(
            parameters=params,
            final_equity_usdt=self.starting_equity,
            total_return_pct=Decimal("0"),
            max_drawdown_pct=Decimal("0"),
            sharpe=Decimal("0"),
            win_rate=Decimal("0"),
            profit_factor=Decimal("0"),
            trade_count=0,
            score=Decimal("-999"),
            equity_curve=[self.starting_equity],
            trades=[],
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

    @staticmethod
    def _mean_std(values: list[Decimal]) -> tuple[Decimal, Decimal]:
        floats = [float(value) for value in values]
        mean = Decimal(str(statistics.fmean(floats)))
        std = Decimal(str(statistics.pstdev(floats))) if len(floats) > 1 else Decimal("0")
        return mean, std

    @staticmethod
    def _max_drawdown(equity_curve: list[Decimal]) -> Decimal:
        if not equity_curve:
            return Decimal("0")
        peak = equity_curve[0]
        max_dd = Decimal("0")
        for value in equity_curve:
            peak = max(peak, value)
            if peak > 0:
                max_dd = max(max_dd, (peak - value) / peak)
        return max_dd

    @staticmethod
    def _sharpe(equity_curve: list[Decimal], interval: str) -> Decimal:
        if len(equity_curve) < 3:
            return Decimal("0")
        returns: list[float] = []
        for previous, current in zip(equity_curve, equity_curve[1:]):
            if previous > 0:
                returns.append(float((current - previous) / previous))
        if len(returns) < 2:
            return Decimal("0")
        std = statistics.pstdev(returns)
        if std <= 0:
            return Decimal("0")
        bars_per_year = (365 * 24 * 60 * 60 * 1000) / INTERVAL_MS.get(interval, 5 * 60_000)
        sharpe = statistics.fmean(returns) / std * math.sqrt(bars_per_year)
        return Decimal(str(sharpe))

    @staticmethod
    def _score(
        total_return: Decimal,
        max_drawdown: Decimal,
        sharpe: Decimal,
        win_rate: Decimal,
        profit_factor: Decimal,
        trade_count: int,
    ) -> Decimal:
        if trade_count < 3:
            return Decimal("-10") + total_return
        clipped_pf = min(profit_factor, Decimal("4"))
        clipped_sharpe = max(Decimal("-3"), min(sharpe, Decimal("3")))
        return (
            total_return * Decimal("100")
            - max_drawdown * Decimal("160")
            + clipped_sharpe * Decimal("1.5")
            + win_rate * Decimal("5")
            + clipped_pf * Decimal("1.25")
            - Decimal("0.03") * Decimal(max(0, trade_count - 120))
        )


class StrategyCalibrator:
    def __init__(self, starting_equity_usdt: Decimal) -> None:
        self.backtester = MeanReversionBacktester(starting_equity_usdt)

    def calibrate(self, candles: list[Candle], interval: str) -> list[BacktestResult]:
        candidates: list[BacktestResult] = []
        candidates.append(
            self.backtester.run(
                candles,
                StrategyParameters(
                    bollinger_window=80,
                    bollinger_stddev=Decimal("2.0"),
                    z_entry=Decimal("1.5"),
                    z_exit=Decimal("0.0"),
                    stop_loss_pct=Decimal("0.025"),
                    take_profit_pct=Decimal("0.040"),
                    max_hold_bars=80,
                    allocation_fraction=Decimal("0"),
                    fee_rate=Decimal("0.001"),
                    strategy_type="no_trade",
                ),
                interval,
            )
        )
        for window in (40, 64, 96, 144):
            for stddev in (Decimal("1.7"), Decimal("2.1"), Decimal("2.5")):
                for z_entry in (Decimal("1.15"), Decimal("1.45"), Decimal("1.80")):
                    for z_exit in (Decimal("0.00"), Decimal("0.35")):
                        for stop_loss in (Decimal("0.018"), Decimal("0.032")):
                            for take_profit in (Decimal("0.018"), Decimal("0.040")):
                                params = StrategyParameters(
                                    bollinger_window=window,
                                    bollinger_stddev=stddev,
                                    z_entry=z_entry,
                                    z_exit=z_exit,
                                    stop_loss_pct=stop_loss,
                                    take_profit_pct=take_profit,
                                    max_hold_bars=max(12, int(window * 1.25)),
                                    allocation_fraction=Decimal("0.12"),
                                    fee_rate=Decimal("0.001"),
                                )
                                candidates.append(self.backtester.run(candles, params, interval))
        for fast, slow in ((12, 48), (20, 80), (34, 144), (50, 200)):
            for stop_loss in (Decimal("0.018"), Decimal("0.032"), Decimal("0.050")):
                for take_profit in (Decimal("0.025"), Decimal("0.050"), Decimal("0.090")):
                    params = StrategyParameters(
                        bollinger_window=slow,
                        bollinger_stddev=Decimal("2.0"),
                        z_entry=Decimal("1.5"),
                        z_exit=Decimal("0.0"),
                        stop_loss_pct=stop_loss,
                        take_profit_pct=take_profit,
                        max_hold_bars=max(24, int(slow * 1.5)),
                        allocation_fraction=Decimal("0.12"),
                        fee_rate=Decimal("0.001"),
                        strategy_type="trend_following",
                        trend_fast_window=fast,
                        trend_slow_window=slow,
                    )
                    candidates.append(self.backtester.run(candles, params, interval))
        ranked = sorted(candidates, key=lambda result: result.score, reverse=True)
        tradable = [
            result for result in ranked
            if result.parameters.strategy_type != "no_trade"
            and result.total_return_pct > 0
            and result.sharpe > 0
            and result.trade_count >= 3
        ]
        no_trade = [result for result in ranked if result.parameters.strategy_type == "no_trade"]
        rejected = [result for result in ranked if result not in tradable and result not in no_trade]
        return tradable + no_trade + rejected


def build_research_profile(
    *,
    symbol: str,
    interval: str,
    days: int,
    candles: list[Candle],
    ranked_results: list[BacktestResult],
) -> JsonDict:
    best = ranked_results[0] if ranked_results else MeanReversionBacktester()._empty_result(
        StrategyParameters(
            bollinger_window=80,
            bollinger_stddev=Decimal("2.0"),
            z_entry=Decimal("1.2"),
            z_exit=Decimal("0.3"),
            stop_loss_pct=Decimal("0.03"),
            take_profit_pct=Decimal("0.04"),
            max_hold_bars=100,
            allocation_fraction=Decimal("0.12"),
            fee_rate=Decimal("0.001"),
        )
    )
    close_values = [candle.close for candle in candles]
    live_trading_allowed = (
        best.parameters.strategy_type != "no_trade"
        and best.total_return_pct > 0
        and best.sharpe > 0
        and best.trade_count >= 3
    )
    if live_trading_allowed:
        permission_reason = "selected strategy produced positive return, positive Sharpe, and enough trades in calibration"
    else:
        permission_reason = "no calibrated strategy passed positive-return and positive-Sharpe gates; hold cash for this symbol"
    profile = {
        "profile_version": 1,
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "symbol": symbol,
        "interval": interval,
        "lookback_days": days,
        "candle_count": len(candles),
        "data_range": {
            "first_open_time_ms": candles[0].open_time_ms if candles else None,
            "last_close_time_ms": candles[-1].close_time_ms if candles else None,
        },
        "source": {
            "name": "MEXC Spot API /api/v3/klines",
            "documentation": "https://www.mexc.com/api-docs/spot-v3/market-data-endpoints/klinecandlestick-data",
        },
        "trade_permission": {
            "paper_trading_allowed": live_trading_allowed,
            "live_trading_allowed": live_trading_allowed,
            "reason": permission_reason,
        },
        "strategy_parameters": best.parameters.as_dict(),
        "backtest": best.as_dict(include_curve=True, include_trades=True),
        "top_candidates": [result.as_dict() for result in ranked_results[:12]],
        "market_summary": {
            "first_close": decimal_float(close_values[0]) if close_values else 0.0,
            "last_close": decimal_float(close_values[-1]) if close_values else 0.0,
            "close_return_pct": decimal_float((close_values[-1] - close_values[0]) / close_values[0]) if len(close_values) > 1 and close_values[0] > 0 else 0.0,
            "average_quote_volume": decimal_float(sum((candle.quote_volume for candle in candles), Decimal("0")) / Decimal(len(candles))) if candles else 0.0,
        },
        "limitations": [
            "Historical calibration is not a profit guarantee.",
            "Backtests do not fully model latency, queue position, partial fills, spread expansion, exchange outages, or regime changes.",
            "Use paper mode and small tier progression before enabling live execution.",
        ],
    }
    return profile


def save_research_profile(profile: JsonDict, path: str) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{target.name}.", dir=str(target.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(profile, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(tmp_name, target)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)


async def run_research(settings: Settings, *, symbol: str | None = None, interval: str | None = None, days: int | None = None) -> JsonDict:
    chosen_symbol = (symbol or settings.trading_symbol).strip().upper()
    chosen_interval = interval or settings.research_default_interval
    chosen_days = days or settings.research_default_days
    async with MexcRESTClient(settings) as rest_client:
        collector = HistoricalDataCollector(rest_client)
        candles = await collector.fetch_candles(chosen_symbol, chosen_interval, chosen_days)
    calibrator = StrategyCalibrator(settings.paper_starting_equity_usdt)
    ranked = calibrator.calibrate(candles, chosen_interval)
    profile = build_research_profile(
        symbol=chosen_symbol,
        interval=chosen_interval,
        days=chosen_days,
        candles=candles,
        ranked_results=ranked,
    )
    save_research_profile(profile, settings.research_profile_file(chosen_symbol))
    return profile


async def run_research_for_symbols(settings: Settings, *, symbols: list[str] | None = None, interval: str | None = None, days: int | None = None) -> list[JsonDict]:
    profiles: list[JsonDict] = []
    for symbol in symbols or list(settings.trading_symbols):
        profiles.append(await run_research(settings, symbol=symbol, interval=interval, days=days))
    return profiles
