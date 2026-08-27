from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--metrics", default="reports/metrics.json")
    parser.add_argument("--out", default="reports/generated_summary.md")
    args = parser.parse_args()
    metrics = json.loads(Path(args.metrics).read_text(encoding="utf-8"))
    lines = [
        "# Day 10 Reliability Final Report",
        "",
        "## Metrics Summary",
        "",
        "| Metric | Value |",
        "|---|---:|",
    ]
    for key, value in metrics.items():
        if key == "scenarios":
            continue
        lines.append(f"| {key} | {value} |")
    lines += ["", "## Chaos Scenarios", "", "| Scenario | Status |", "|---|---|"]
    for key, value in metrics.get("scenarios", {}).items():
        lines.append(f"| {key} | {value} |")
    availability = float(metrics.get("availability", 0.0))
    cache_rate = float(metrics.get("cache_hit_rate", 0.0))
    saved = float(metrics.get("estimated_cost_saved", 0.0))
    spent = float(metrics.get("estimated_cost", 0.0))
    opens = metrics.get("circuit_open_count", 0)
    recovery = metrics.get("recovery_time_ms")
    failed = [name for name, status in metrics.get("scenarios", {}).items() if status != "pass"]

    lines += [
        "",
        "## Analysis",
        "",
        (
            f"- Availability across all scenarios: {availability:.2%}"
            f" (error rate {float(metrics.get('error_rate', 0.0)):.2%})."
        ),
        (
            f"- The cache served {cache_rate:.2%} of requests, saving an estimated"
            f" {saved:.4f} against {spent:.4f} actually spent."
        ),
        f"- Circuits opened {opens} time(s);"
        + (
            f" average recovery took {float(recovery):.0f} ms."
            if recovery is not None
            else " no open circuit recovered during this run."
        ),
        "- Scenario results: "
        + ("all scenarios passed." if not failed else f"failed: {', '.join(failed)}."),
        "",
        "This file is generated from `metrics.json`. The written analysis (architecture,",
        "configuration rationale, chaos evidence, and the remaining failure mode) lives in",
        "`reports/final_report.md`.",
    ]
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text("\n".join(lines), encoding="utf-8")
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
