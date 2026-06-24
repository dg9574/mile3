from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.config import get_settings
from backend.research import run_research_for_symbols


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download MEXC candles and calibrate the ProjectMile3 strategy profile.")
    parser.add_argument("--symbols", default=None, help="Comma-separated symbols. Defaults to TRADING_SYMBOLS or TRADING_SYMBOL.")
    parser.add_argument("--interval", default=None, help="MEXC kline interval, for example 1m, 5m, 15m, 60m, 4h, 1d.")
    parser.add_argument("--days", type=int, default=None, help="Lookback window in days. Defaults to RESEARCH_DEFAULT_DAYS.")
    parser.add_argument("--print-json", action="store_true", help="Print the complete generated profile.")
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    settings = get_settings()
    symbols = [item.strip().upper() for item in args.symbols.split(",") if item.strip()] if args.symbols else list(settings.trading_symbols)
    profiles = await run_research_for_symbols(settings, symbols=symbols, interval=args.interval, days=args.days)
    if args.print_json:
        print(json.dumps(profiles, indent=2, sort_keys=True))
    else:
        for profile in profiles:
            backtest = profile.get("backtest", {})
            params = profile.get("strategy_parameters", {})
            permission = profile.get("trade_permission", {})
            path = Path(settings.research_profile_file(str(profile.get("symbol")))).resolve()
            print(f"Research profile saved: {path}")
            print(f"Symbol: {profile.get('symbol')}  Interval: {profile.get('interval')}  Candles: {profile.get('candle_count')}")
            print(
                "Backtest: "
                f"return={float(backtest.get('total_return_pct') or 0) * 100:.2f}% "
                f"drawdown={float(backtest.get('max_drawdown_pct') or 0) * 100:.2f}% "
                f"sharpe={float(backtest.get('sharpe') or 0):.2f} "
                f"trades={backtest.get('trade_count')}"
            )
            print(
                "Selected params: "
                f"strategy={params.get('strategy_type')} "
                f"window={params.get('bollinger_window')} "
                f"std={params.get('bollinger_stddev')} "
                f"z_entry={params.get('z_entry')} "
                f"z_exit={params.get('z_exit')} "
                f"fast={params.get('trend_fast_window')} "
                f"slow={params.get('trend_slow_window')}"
            )
            print(f"Trade gate: {'OPEN' if permission.get('paper_trading_allowed') else 'CLOSED'} - {permission.get('reason')}")
            print("")


if __name__ == "__main__":
    asyncio.run(main())
