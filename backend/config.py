from __future__ import annotations

from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class CapitalTier(BaseModel):
    tier: int
    name: str
    required_equity_usdt: Decimal
    allocation_limit: Decimal
    trailing_stop_variance_factor: Decimal
    monthly_progression_threshold: Decimal

    model_config = {"frozen": True}


CAPITAL_TIER_PROGRESSION: tuple[CapitalTier, ...] = (
    CapitalTier(
        tier=1,
        name="TIER_1",
        required_equity_usdt=Decimal("100"),
        allocation_limit=Decimal("0.1200"),
        trailing_stop_variance_factor=Decimal("1.10"),
        monthly_progression_threshold=Decimal("0.0700"),
    ),
    CapitalTier(
        tier=2,
        name="TIER_2",
        required_equity_usdt=Decimal("1000"),
        allocation_limit=Decimal("0.1500"),
        trailing_stop_variance_factor=Decimal("1.25"),
        monthly_progression_threshold=Decimal("0.0600"),
    ),
    CapitalTier(
        tier=3,
        name="TIER_3",
        required_equity_usdt=Decimal("3000"),
        allocation_limit=Decimal("0.1700"),
        trailing_stop_variance_factor=Decimal("1.40"),
        monthly_progression_threshold=Decimal("0.0500"),
    ),
    CapitalTier(
        tier=4,
        name="TIER_4",
        required_equity_usdt=Decimal("5000"),
        allocation_limit=Decimal("0.1850"),
        trailing_stop_variance_factor=Decimal("1.60"),
        monthly_progression_threshold=Decimal("0.0450"),
    ),
    CapitalTier(
        tier=5,
        name="TIER_5",
        required_equity_usdt=Decimal("10000"),
        allocation_limit=Decimal("0.2000"),
        trailing_stop_variance_factor=Decimal("1.85"),
        monthly_progression_threshold=Decimal("0.0400"),
    ),
)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
        validate_default=True,
    )

    mexc_api_key: SecretStr | None = Field(default=None, alias="MEXC_API_KEY")
    mexc_secret_key: SecretStr | None = Field(default=None, alias="MEXC_SECRET_KEY")
    openai_api_key: SecretStr | None = Field(default=None, alias="OPENAI_API_KEY")
    trading_symbol: str = Field(default="BTCUSDT", alias="TRADING_SYMBOL")
    trading_symbols_raw: str | None = Field(default=None, alias="TRADING_SYMBOLS")

    rest_base_url: str = Field(default="https://api.mexc.com", alias="MEXC_REST_BASE_URL")
    ws_base_url: str = Field(default="wss://wbs-api.mexc.com/ws", alias="MEXC_WS_BASE_URL")
    openai_model: str = Field(default="gpt-5.5", alias="OPENAI_MODEL")
    ai_provider: Literal["local", "openai", "ollama"] = Field(default="local", alias="AI_PROVIDER")
    ollama_base_url: str = Field(default="http://127.0.0.1:11434", alias="OLLAMA_BASE_URL")
    ollama_model: str = Field(default="llama3.1:8b", alias="OLLAMA_MODEL")
    app_access_token: SecretStr | None = Field(default=None, alias="APP_ACCESS_TOKEN")
    app_name: str = Field(default="ProjectMile3 MEXC Quant Engine", alias="APP_NAME")
    trading_mode: Literal["disabled", "paper", "live"] = Field(default="paper", alias="TRADING_MODE")
    live_trading_confirmation: str = Field(default="", alias="LIVE_TRADING_CONFIRMATION")
    auto_live_after_train: bool = Field(default=True, alias="AUTO_LIVE_AFTER_TRAIN")
    auto_train_on_start: bool = Field(default=False, alias="AUTO_TRAIN_ON_START")

    hard_daily_drawdown_limit_pct: Decimal = Field(
        default=Decimal("0.0300"),
        alias="HARD_DAILY_DRAWDOWN_LIMIT_PCT",
        description="Hard maximum daily drawdown. Default is 3.0%.",
    )
    max_capital_utilization_per_position: Decimal = Field(
        default=Decimal("0.2000"),
        alias="MAX_CAPITAL_UTILIZATION_PER_POSITION",
    )
    trailing_stop_floor_variance_factor: Decimal = Field(
        default=Decimal("0.75"),
        alias="TRAILING_STOP_FLOOR_VARIANCE_FACTOR",
    )
    trailing_stop_base_variance_factor: Decimal = Field(
        default=Decimal("1.25"),
        alias="TRAILING_STOP_BASE_VARIANCE_FACTOR",
    )
    trailing_stop_ceiling_variance_factor: Decimal = Field(
        default=Decimal("2.75"),
        alias="TRAILING_STOP_CEILING_VARIANCE_FACTOR",
    )

    recv_window_ms: int = Field(default=5000, alias="MEXC_RECV_WINDOW_MS")
    rest_rate_limit_weight_per_10s: int = Field(default=250, alias="MEXC_REST_WEIGHT_PER_10S")
    rest_timeout_seconds: float = Field(default=10.0, alias="MEXC_REST_TIMEOUT_SECONDS")
    rest_poll_interval_seconds: float = Field(default=8.0, alias="REST_POLL_INTERVAL_SECONDS")
    ws_ping_interval_seconds: float = Field(default=20.0, alias="WS_PING_INTERVAL_SECONDS")
    ws_stale_timeout_seconds: float = Field(default=75.0, alias="WS_STALE_TIMEOUT_SECONDS")
    ws_max_connection_age_seconds: float = Field(default=85500.0, alias="WS_MAX_CONNECTION_AGE_SECONDS")
    ws_initial_backoff_seconds: float = Field(default=1.0, alias="WS_INITIAL_BACKOFF_SECONDS")
    ws_max_backoff_seconds: float = Field(default=60.0, alias="WS_MAX_BACKOFF_SECONDS")

    bollinger_window: int = Field(default=80, alias="BOLLINGER_WINDOW")
    bollinger_stddev: Decimal = Field(default=Decimal("2.0"), alias="BOLLINGER_STDDEV")
    min_signal_confidence: Decimal = Field(default=Decimal("0.58"), alias="MIN_SIGNAL_CONFIDENCE")
    min_live_execution_confidence: Decimal = Field(
        default=Decimal("0.74"),
        alias="MIN_LIVE_EXECUTION_CONFIDENCE",
    )
    kelly_fraction_cap: Decimal = Field(default=Decimal("0.2500"), alias="KELLY_FRACTION_CAP")
    volatility_position_target: Decimal = Field(default=Decimal("0.0200"), alias="VOLATILITY_POSITION_TARGET")
    paper_starting_equity_usdt: Decimal = Field(default=Decimal("100"), alias="PAPER_STARTING_EQUITY_USDT")
    minimum_trade_notional_usdt: Decimal = Field(default=Decimal("5"), alias="MINIMUM_TRADE_NOTIONAL_USDT")
    max_active_positions: int = Field(default=3, alias="MAX_ACTIVE_POSITIONS")
    quote_reserve_fraction: Decimal = Field(default=Decimal("0.35"), alias="QUOTE_RESERVE_FRACTION")
    portfolio_rebalance_interval_seconds: float = Field(default=60.0, alias="PORTFOLIO_REBALANCE_INTERVAL_SECONDS")
    portfolio_min_confidence: Decimal = Field(default=Decimal("0.62"), alias="PORTFOLIO_MIN_CONFIDENCE")
    portfolio_rotation_sell_confidence: Decimal = Field(default=Decimal("0.67"), alias="PORTFOLIO_ROTATION_SELL_CONFIDENCE")
    state_path: str = Field(default="data/engine_state.json", alias="ENGINE_STATE_PATH")
    portfolio_state_path: str = Field(default="data/portfolio_state.json", alias="PORTFOLIO_STATE_PATH")
    research_profile_path: str = Field(default="data/research_profile.json", alias="RESEARCH_PROFILE_PATH")
    research_profiles_dir: str = Field(default="data/research_profiles", alias="RESEARCH_PROFILES_DIR")
    research_default_interval: str = Field(default="5m", alias="RESEARCH_DEFAULT_INTERVAL")
    research_default_days: int = Field(default=30, alias="RESEARCH_DEFAULT_DAYS")
    training_min_open_symbols: int = Field(default=1, alias="TRAINING_MIN_OPEN_SYMBOLS")
    file_analysis_root: str = Field(default="data/user_files", alias="FILE_ANALYSIS_ROOT")
    max_file_analysis_chars: int = Field(default=16000, alias="MAX_FILE_ANALYSIS_CHARS")

    @field_validator("trading_symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        symbol = value.strip().upper()
        if not symbol.isalnum() or len(symbol) < 6:
            raise ValueError("TRADING_SYMBOL must be an uppercase exchange symbol such as BTCUSDT")
        return symbol

    @field_validator(
        "hard_daily_drawdown_limit_pct",
        "max_capital_utilization_per_position",
        "trailing_stop_floor_variance_factor",
        "trailing_stop_base_variance_factor",
        "trailing_stop_ceiling_variance_factor",
        "bollinger_stddev",
        "min_signal_confidence",
        "min_live_execution_confidence",
        "kelly_fraction_cap",
        "volatility_position_target",
        "quote_reserve_fraction",
        "portfolio_min_confidence",
        "portfolio_rotation_sell_confidence",
    )
    @classmethod
    def validate_positive_decimal(cls, value: Decimal) -> Decimal:
        if value <= 0:
            raise ValueError("risk parameters must be positive")
        return value

    @field_validator("recv_window_ms")
    @classmethod
    def validate_recv_window(cls, value: int) -> int:
        if not 1000 <= value <= 60000:
            raise ValueError("MEXC_RECV_WINDOW_MS must be between 1000 and 60000")
        return value

    @property
    def capital_tiers(self) -> tuple[CapitalTier, ...]:
        return CAPITAL_TIER_PROGRESSION

    @property
    def trading_symbols(self) -> tuple[str, ...]:
        raw = self.trading_symbols_raw or self.trading_symbol
        symbols: list[str] = []
        for item in raw.replace(";", ",").split(","):
            symbol = item.strip().upper()
            if not symbol:
                continue
            if not symbol.isalnum() or len(symbol) < 6:
                raise ValueError(f"Invalid configured trading symbol: {symbol}")
            if symbol not in symbols:
                symbols.append(symbol)
        return tuple(symbols or [self.trading_symbol])

    @property
    def has_mexc_credentials(self) -> bool:
        return bool(self.mexc_api_key and self.mexc_secret_key)

    @property
    def has_openai_credentials(self) -> bool:
        return bool(self.openai_api_key)

    @property
    def live_trading_armed(self) -> bool:
        return self.trading_mode == "live" and self.live_trading_confirmation == "I_UNDERSTAND_LIVE_MEXC_TRADING"

    @property
    def base_asset(self) -> str:
        return split_symbol(self.trading_symbol)[0]

    @property
    def quote_asset(self) -> str:
        return split_symbol(self.trading_symbol)[1]

    def research_profile_file(self, symbol: str) -> str:
        normalized = symbol.strip().upper()
        if len(self.trading_symbols) == 1 and normalized == self.trading_symbol:
            return self.research_profile_path
        return str(Path(self.research_profiles_dir) / f"{normalized}.json")

    def tier_for_equity(self, equity_usdt: Decimal) -> CapitalTier:
        active = CAPITAL_TIER_PROGRESSION[0]
        for tier in CAPITAL_TIER_PROGRESSION:
            if equity_usdt >= tier.required_equity_usdt:
                active = tier
        return active

    def next_tier(self, tier: CapitalTier) -> CapitalTier | None:
        index = tier.tier
        if index >= len(CAPITAL_TIER_PROGRESSION):
            return None
        return CAPITAL_TIER_PROGRESSION[index]

    def effective_allocation_limit(self, tier: CapitalTier) -> Decimal:
        return min(tier.allocation_limit, self.max_capital_utilization_per_position)

    def trailing_stop_variance_factor(self, tier: CapitalTier, realized_volatility: Decimal) -> Decimal:
        volatility_adjustment = self.trailing_stop_base_variance_factor + realized_volatility
        tier_adjusted = tier.trailing_stop_variance_factor * volatility_adjustment
        return min(
            self.trailing_stop_ceiling_variance_factor,
            max(self.trailing_stop_floor_variance_factor, tier_adjusted),
        )


def reveal_secret(value: SecretStr | None) -> str | None:
    if value is None:
        return None
    secret = value.get_secret_value().strip()
    return secret or None


def split_symbol(symbol: str) -> tuple[str, str]:
    normalized = symbol.strip().upper()
    for quote in ("USDT", "USDC", "BTC", "ETH", "MX"):
        if normalized.endswith(quote) and len(normalized) > len(quote):
            return normalized[: -len(quote)], quote
    return normalized[:-4], normalized[-4:]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
