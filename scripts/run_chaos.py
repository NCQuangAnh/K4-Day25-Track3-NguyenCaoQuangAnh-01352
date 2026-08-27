from __future__ import annotations

import argparse
import random

from reliability_lab.chaos import load_queries, run_simulation
from reliability_lab.config import load_config


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="configs/default.yaml")
    parser.add_argument("--out", default="reports/metrics.json")
    parser.add_argument("--seed", type=int, default=7, help="RNG seed for reproducible runs")
    args = parser.parse_args()
    random.seed(args.seed)
    config = load_config(args.config)
    metrics = run_simulation(config, load_queries(), seed=args.seed)
    metrics.write_json(args.out)
    metrics.write_csv(args.out.replace(".json", ".csv"))
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
