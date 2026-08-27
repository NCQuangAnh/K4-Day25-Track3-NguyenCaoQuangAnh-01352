"""Collect per-scenario metrics, cache comparison, and Redis evidence for the report."""
from __future__ import annotations

import json
import random
from pathlib import Path

from reliability_lab.chaos import build_gateway, load_queries, run_scenario
from reliability_lab.config import ScenarioConfig, load_config
from reliability_lab.metrics import RunMetrics


def summarize(name: str, metrics: RunMetrics) -> dict[str, object]:
    report = metrics.to_report_dict()
    report.pop("scenarios", None)
    report["scenario"] = name
    report["cache_hits"] = metrics.cache_hits
    report["fallback_successes"] = metrics.fallback_successes
    report["static_fallbacks"] = metrics.static_fallbacks
    return report


def main() -> None:
    random.seed(42)
    config = load_config("configs/default.yaml")
    queries = load_queries()

    results: dict[str, object] = {}

    for scenario in config.scenarios:
        results[scenario.name] = summarize(scenario.name, run_scenario(config, queries, scenario))

    no_cache = config.model_copy(deep=True)
    no_cache.cache.enabled = False
    results["all_healthy_no_cache"] = summarize(
        "all_healthy_no_cache",
        run_scenario(no_cache, queries, ScenarioConfig(name="all_healthy")),
    )

    redis_config = config.model_copy(deep=True)
    redis_config.cache.backend = "redis"
    gateway = build_gateway(redis_config, None)
    if gateway.cache is not None:
        gateway.cache.flush()  # type: ignore[union-attr]
    results["all_healthy_redis"] = summarize(
        "all_healthy_redis",
        run_scenario(redis_config, queries, ScenarioConfig(name="all_healthy")),
    )

    Path("reports").mkdir(exist_ok=True)
    Path("reports/scenario_metrics.json").write_text(json.dumps(results, indent=2))
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
