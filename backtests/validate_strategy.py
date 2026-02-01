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
from adapters.variety_adapter import DummyAdapter


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

	# 中文输出：先结论，再细节
	print("结论：")
	last_value = metrics.get("last_value")
	max_dd = metrics.get("max_drawdown")
	sharpe = metrics.get("sharpe")
	if isinstance(last_value, float) and isinstance(max_dd, float) and isinstance(sharpe, float):
		conclusion = (
			f"策略累计净值约为 {last_value:.2f}，最大回撤约为 {max_dd:.2%}，"
			f"夏普比率约为 {sharpe:.2f}。"
		)
	else:
		conclusion = "已生成回测结果，请查看指标明细。"
	print(f"  {conclusion}")

	print("指标明细：")
	cn_labels = {
		"cagr": "年化收益率",
		"vol": "波动率",
		"sharpe": "夏普比率",
		"max_drawdown": "最大回撤",
		"last_value": "期末净值",
	}
	for k, v in metrics.items():
		label = cn_labels.get(k, k)
		if isinstance(v, float):
			if k in {"cagr", "vol", "max_drawdown"}:
				print(f"  {label}: {v:.2%}")
			else:
				print(f"  {label}: {v:.4f}")
		else:
			print(f"  {label}: {v}")

	adapter = DummyAdapter()
	quote_stub = adapter.fetch_quote(equity_symbol)
	print("Adapter (stub) quote example:", quote_stub)


if __name__ == "__main__":
	run_validation()
