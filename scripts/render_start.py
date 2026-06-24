from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

import uvicorn

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.config import get_settings
from backend.research import run_research_for_symbols


async def ensure_profiles() -> None:
    settings = get_settings()
    if not settings.auto_train_on_start:
        print("AUTO_TRAIN_ON_START is false. Starting app; use the dashboard Train button to calibrate.", flush=True)
        return
    missing = [
        symbol for symbol in settings.trading_symbols
        if not Path(settings.research_profile_file(symbol)).exists()
    ]
    if not missing:
        print("Research profiles already present. Starting app.", flush=True)
        return
    print(f"Missing research profiles for {', '.join(missing)}. Training before startup.", flush=True)
    await run_research_for_symbols(
        settings,
        symbols=missing,
        interval=settings.research_default_interval,
        days=settings.research_default_days,
    )
    print("Research profile training complete. Starting app.", flush=True)


def main() -> None:
    asyncio.run(ensure_profiles())
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
