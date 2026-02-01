"""Quick validation script for gold vs A-share ETF rotation."""

import sys
from pathlib import Path
from typing import Optional

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
	sys.path.append(str(ROOT))

from src.data_fetcher import (
	fetch_a_share_index_or_etf,
	fetch_gold_futures,
	save_data,
)
from strategies.rotation_strategy import RotationConfig, generate_signals, performance_summary


def run_validation(
	equity_symbol: str = "510300",
	start_date: str = "2015-01-01",
	end_date: Optional[str] = None,
	lookback_days: int = 60,
	rebalance: str = "weekly",
	fee_bps: float = 5.0,
):
	gold = fetch_gold_futures(start_date=start_date, end_date=end_date)
	equity = fetch_a_share_index_or_etf(symbol=equity_symbol, start_date=start_date, end_date=end_date)

	cfg = RotationConfig(lookback_days=lookback_days, rebalance=rebalance, fee_bps=fee_bps)
	result = generate_signals(gold, equity, config=cfg)

	curve = (1 + result["portfolio_ret"]).cumprod()
	metrics = performance_summary(result["portfolio_ret"])

	output = pd.concat([result, curve.rename("portfolio_curve")], axis=1)
	csv_path = save_data(output.reset_index(drop=True), f"backtest_{equity_symbol}.csv")

	print(f"Saved backtest results to {csv_path}")
	print("Metrics:")
	for k, v in metrics.items():
		print(f"  {k}: {v:.4f}" if isinstance(v, float) else f"  {k}: {v}")


if __name__ == "__main__":
	run_validation()
