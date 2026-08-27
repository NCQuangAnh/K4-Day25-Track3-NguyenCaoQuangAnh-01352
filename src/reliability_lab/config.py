from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


class ProviderConfig(BaseModel):
    name: str
    fail_rate: float = Field(ge=0.0, le=1.0)
    base_latency_ms: int = Field(gt=0)
    cost_per_1k_tokens: float = Field(ge=0.0)


class CircuitBreakerConfig(BaseModel):
    failure_threshold: int = Field(gt=0)
    reset_timeout_seconds: float = Field(gt=0)
    success_threshold: int = Field(gt=0)


class CacheConfig(BaseModel):
    enabled: bool = True
    backend: str = "memory"  # "memory" or "redis"
    ttl_seconds: int = Field(gt=0)
    similarity_threshold: float = Field(ge=0.0, le=1.0)
    redis_url: str = "redis://localhost:6379/0"


class LoadTestConfig(BaseModel):
    requests: int = Field(gt=0)
    concurrency: int = Field(default=1, gt=0)  # 1 = sequential; >1 uses a ThreadPoolExecutor


class BudgetConfig(BaseModel):
    """Cost cap for a run.  limit=None disables cost-aware routing entirely."""

    limit: float | None = None
    degrade_ratio: float = Field(default=0.8, gt=0.0, le=1.0)


class ScenarioConfig(BaseModel):
    name: str
    description: str = ""
    provider_overrides: dict[str, float] = Field(default_factory=dict)
    budget_limit: float | None = None  # overrides LabConfig.budget.limit for this scenario
    concurrency: int | None = None  # overrides LoadTestConfig.concurrency for this scenario


class LabConfig(BaseModel):
    providers: list[ProviderConfig]
    circuit_breaker: CircuitBreakerConfig
    cache: CacheConfig
    load_test: LoadTestConfig
    budget: BudgetConfig = Field(default_factory=BudgetConfig)
    scenarios: list[ScenarioConfig] = Field(default_factory=list)


def load_config(path: str | Path) -> LabConfig:
    raw: dict[str, Any] = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return LabConfig.model_validate(raw)
