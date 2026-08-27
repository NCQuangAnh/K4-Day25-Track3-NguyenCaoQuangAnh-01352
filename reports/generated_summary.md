# Day 10 Reliability Final Report

## Metrics Summary

| Metric | Value |
|---|---:|
| total_requests | 500 |
| availability | 0.914 |
| error_rate | 0.086 |
| latency_p50_ms | 269.5 |
| latency_p95_ms | 313.56 |
| latency_p99_ms | 319.83 |
| fallback_success_rate | 0.6972 |
| cache_hit_rate | 0.594 |
| circuit_open_count | 10 |
| recovery_time_ms | 2226.520538330078 |
| estimated_cost | 0.071254 |
| estimated_cost_saved | 0.297 |
| concurrency | 8 |
| coalesced_waits | 22 |
| coalesced_hits | 12 |
| budget_limit | None |
| budget_spent | 0.071254 |

## Chaos Scenarios

| Scenario | Status |
|---|---|
| no_cache_baseline | pass |
| primary_timeout_100 | pass |
| primary_flaky_50 | pass |
| all_healthy | pass |
| cost_budget_squeeze | pass |
| concurrent_load_8 | pass |

## Analysis

- Availability across all scenarios: 91.40% (error rate 8.60%).
- The cache served 59.40% of requests, saving an estimated 0.2970 against 0.0713 actually spent.
- Circuits opened 10 time(s); average recovery took 2227 ms.
- Scenario results: all scenarios passed.

This file is generated from `metrics.json`. The written analysis (architecture,
configuration rationale, chaos evidence, and the remaining failure mode) lives in
`reports/final_report.md`.