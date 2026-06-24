from __future__ import annotations

from decimal import Decimal

from backend.advanced_engine import AdvancedTradingEngine
from backend.config import Settings
from backend.portfolio import PortfolioTradingEngine
from backend.research import Candle, MeanReversionBacktester, StrategyParameters


def test_order_book_imbalance_and_telemetry(tmp_path):
    settings = Settings(
        TRADING_SYMBOL="BTCUSDT",
        TRADING_SYMBOLS="BTCUSDT",
        ENGINE_STATE_PATH=str(tmp_path / "state.json"),
        RESEARCH_PROFILE_PATH=str(tmp_path / "research.json"),
        RESEARCH_PROFILES_DIR=str(tmp_path / "profiles"),
        PAPER_STARTING_EQUITY_USDT=Decimal("100"),
    )
    engine = AdvancedTradingEngine(settings)

    signal = engine.refresh_depth_snapshot(
        {
            "bids": [["100", "3"], ["99", "2"], ["98", "1"], ["97", "1"], ["96", "1"]],
            "asks": [["101", "1"], ["102", "1"], ["103", "1"], ["104", "1"], ["105", "1"]],
        }
    )
    telemetry = engine.telemetry()

    assert signal.action in {"BUY", "SELL", "HOLD", "LOCKED"}
    assert telemetry["symbol"] == "BTCUSDT"
    assert telemetry["system_balance"]["total_equity_usdt"] == 100.0
    assert telemetry["market"]["order_book"]["imbalance"] > 0


def test_daily_drawdown_lockout(tmp_path):
    settings = Settings(
        TRADING_SYMBOL="BTCUSDT",
        TRADING_SYMBOLS="BTCUSDT",
        ENGINE_STATE_PATH=str(tmp_path / "state.json"),
        RESEARCH_PROFILE_PATH=str(tmp_path / "research.json"),
        RESEARCH_PROFILES_DIR=str(tmp_path / "profiles"),
        PAPER_STARTING_EQUITY_USDT=Decimal("100"),
    )
    engine = AdvancedTradingEngine(settings)
    engine.refresh_depth_snapshot({"bids": [["100", "1"]], "asks": [["101", "1"]]})
    assert not engine.telemetry()["tier_progression"]["trading_locked"]

    engine.balances[settings.quote_asset] = {"free": Decimal("96"), "locked": Decimal("0")}
    telemetry = engine.telemetry()
    assert telemetry["tier_progression"]["trading_locked"]
    assert telemetry["tier_progression"]["daily_drawdown_pct"] >= 0.03


def test_mean_reversion_backtester_runs():
    candles = []
    base = Decimal("100")
    for index in range(180):
        wave = Decimal(index % 20) - Decimal("10")
        close = base + wave * Decimal("0.35")
        candles.append(
            Candle(
                open_time_ms=index * 60_000,
                open=close,
                high=close + Decimal("0.50"),
                low=close - Decimal("0.50"),
                close=close,
                volume=Decimal("10"),
                close_time_ms=index * 60_000 + 59_999,
                quote_volume=Decimal("1000"),
                trade_count=10,
            )
        )
    params = StrategyParameters(
        bollinger_window=40,
        bollinger_stddev=Decimal("2.0"),
        z_entry=Decimal("1.0"),
        z_exit=Decimal("0.2"),
        stop_loss_pct=Decimal("0.05"),
        take_profit_pct=Decimal("0.05"),
        max_hold_bars=40,
        allocation_fraction=Decimal("0.12"),
        fee_rate=Decimal("0.001"),
    )
    result = MeanReversionBacktester().run(candles, params, "1m")
    assert result.final_equity_usdt > 0
    assert result.trade_count >= 1


def test_portfolio_engine_tracks_multiple_symbols(tmp_path):
    settings = Settings(
        TRADING_SYMBOL="BTCUSDT",
        TRADING_SYMBOLS="BTCUSDT,ETHUSDT",
        PORTFOLIO_STATE_PATH=str(tmp_path / "portfolio.json"),
        ENGINE_STATE_PATH=str(tmp_path / "single.json"),
        RESEARCH_PROFILES_DIR=str(tmp_path / "profiles"),
        PAPER_STARTING_EQUITY_USDT=Decimal("100"),
        PORTFOLIO_REBALANCE_INTERVAL_SECONDS=0,
    )
    portfolio = PortfolioTradingEngine(settings)
    portfolio.refresh_depth_snapshot("BTCUSDT", {"bids": [["100", "2"]], "asks": [["101", "1"]]})
    portfolio.refresh_depth_snapshot("ETHUSDT", {"bids": [["50", "2"]], "asks": [["51", "1"]]})
    telemetry = portfolio.telemetry()
    assert telemetry["portfolio"]["enabled"] is True
    assert telemetry["portfolio"]["symbols"] == ["BTCUSDT", "ETHUSDT"]
    assert set(telemetry["symbols"].keys()) == {"BTCUSDT", "ETHUSDT"}
