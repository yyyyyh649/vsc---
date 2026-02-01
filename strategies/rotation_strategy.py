"""Rotation strategy between gold futures and A-share ETF/index."""

from dataclasses import dataclass
from typing import Literal, Optional

import numpy as np
import pandas as pd

Rebalance = Literal["daily", "weekly", "monthly"]


@dataclass
class RotationConfig:
	lookback_days: int = 60
	rebalance: Rebalance = "weekly"
	fee_bps: float = 5.0  # one-way trading cost in basis points
	cash_symbol: str = "CASH"


def _prepare_prices(gold: pd.DataFrame, equity: pd.DataFrame) -> pd.DataFrame:
	gold_series = gold.set_index("Date")["Close"].rename("GOLD")
	equity_series = equity.set_index("Date")["Close"].rename("EQUITY")
	# Align on overlapping trading days only to avoid holiday-induced forward fills.
	prices = pd.concat([gold_series, equity_series], axis=1, join="inner").sort_index()
	prices = prices.dropna()
	return prices


def _rebalance_index(prices: pd.DataFrame, mode: Rebalance) -> pd.DatetimeIndex:
	if mode == "daily":
		return prices.index
	if mode == "weekly":
		return prices.resample("W-FRI").last().index
	if mode == "monthly":
		return prices.resample("M").last().index
	raise ValueError(f"Unsupported rebalance mode: {mode}")


def generate_signals(
	gold: pd.DataFrame,
	equity: pd.DataFrame,
	config: Optional[RotationConfig] = None,
) -> pd.DataFrame:
	"""Create allocation signals using lookback momentum.

	Returns a dataframe with columns: position, gold_ret, equity_ret, portfolio_ret.
	position is one of ["GOLD", "EQUITY", config.cash_symbol].
	"""

	cfg = config or RotationConfig()
	prices = _prepare_prices(gold, equity)
	daily_ret = prices.pct_change().fillna(0.0)

	momentum = prices.pct_change(cfg.lookback_days)
	rebalance_dates = _rebalance_index(prices, cfg.rebalance)
	momentum_reb = momentum.reindex(rebalance_dates).dropna()

	pick = momentum_reb.idxmax(axis=1)
	pick_df = pick.to_frame("position_raw")
	pick_df.loc[
		momentum_reb.max(axis=1) <= 0,
		"position_raw",
	] = cfg.cash_symbol

	# Decision signal at T (using T close), execution at T+1 to avoid look-ahead bias.
	signal_series = pick_df["position_raw"].reindex(daily_ret.index).ffill()
	signal_series = signal_series.fillna(cfg.cash_symbol)

	exec_position = signal_series.shift(1).fillna(cfg.cash_symbol)

	turnover = (exec_position != exec_position.shift(1).fillna(cfg.cash_symbol)).astype(float)
	fee = turnover * (cfg.fee_bps / 10000.0)

	gold_ret = daily_ret["GOLD"].rename("gold_ret")
	equity_ret = daily_ret["EQUITY"].rename("equity_ret")

	alloc_gold = (exec_position == "GOLD").astype(float)
	alloc_equity = (exec_position == "EQUITY").astype(float)

	portfolio_ret = alloc_gold * gold_ret + alloc_equity * equity_ret
	portfolio_ret = portfolio_ret - fee

	return pd.DataFrame(
		{
			"signal": signal_series,
			"position": exec_position,
			"gold_ret": gold_ret,
			"equity_ret": equity_ret,
			"portfolio_ret": portfolio_ret,
		}
	)


def performance_summary(returns: pd.Series) -> dict:
	"""Compute simple performance metrics from daily returns."""

	if returns.empty:
		return {}

	daily_ret = returns
	cum_curve = (1 + daily_ret).cumprod()
	total_days = (cum_curve.index[-1] - cum_curve.index[0]).days
	years = total_days / 365.25 if total_days > 0 else 0

	cagr = cum_curve.iloc[-1] ** (1 / years) - 1 if years > 0 else np.nan
	vol = daily_ret.std() * np.sqrt(252)
	sharpe = (daily_ret.mean() / daily_ret.std()) * np.sqrt(252) if daily_ret.std() != 0 else np.nan
	dd = (cum_curve / cum_curve.cummax() - 1).min()

	return {
		"cagr": cagr,
		"vol": vol,
		"sharpe": sharpe,
		"max_drawdown": dd,
		"last_value": cum_curve.iloc[-1],
	}
