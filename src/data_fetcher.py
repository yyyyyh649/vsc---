# 数据获取模块
# 功能：
# 1. 获取黄金期货历史数据 (GC=F from yfinance)
# 2. 获取A股指数或ETF历史数据 (使用 akshare，例如 000001.SH 上证指数, 510300.SH 沪深300ETF)
# 3. 统一返回 pandas DataFrame，列名：Date, Open, High, Low, Close, Volume
# 4. 支持指定开始日期和结束日期
# 5. 添加保存到 CSV 的函数，路径为 data/ 目录下
# 6. 添加基本的错误处理和数据清洗

from datetime import datetime
from pathlib import Path
from typing import Optional

import akshare as ak
import pandas as pd
import yfinance as yf

DATA_DIR = Path(__file__).resolve().parent.parent / "data"


def _to_datetime(date_str: str) -> datetime:
    """Parse a date string in YYYY-MM-DD format."""

    return datetime.strptime(date_str, "%Y-%m-%d")


def _clean_price_dataframe(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """Standardize price dataframe columns and ordering."""

    if df.empty:
        raise ValueError(f"No data returned for symbol {symbol}")

    df = df.rename(
        columns={
            "日期": "Date",
            "date": "Date",
            "开盘": "Open",
            "open": "Open",
            "最高": "High",
            "high": "High",
            "最低": "Low",
            "low": "Low",
            "收盘": "Close",
            "close": "Close",
            "成交量": "Volume",
            "volume": "Volume",
            "成交量(手)": "Volume",
        }
    )

    # Normalize to date (naive) to reduce timezone drift across sources.
    df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None).dt.normalize()
    df = df[["Date", "Open", "High", "Low", "Close", "Volume"]]
    df[["Open", "High", "Low", "Close", "Volume"]] = df[
        ["Open", "High", "Low", "Close", "Volume"]
    ].apply(pd.to_numeric, errors="coerce")
    df = df.dropna(subset=["Open", "High", "Low", "Close"]).sort_values("Date")
    df["Symbol"] = symbol
    return df.reset_index(drop=True)


def fetch_gold_futures(start_date: str, end_date: Optional[str] = None) -> pd.DataFrame:
    """Fetch COMEX gold futures (GC=F) from Yahoo Finance."""

    start = _to_datetime(start_date)
    end = _to_datetime(end_date) if end_date else None

    data = yf.download(
        "GC=F",
        start=start,
        end=end,
        progress=False,
        auto_adjust=False,
    )

    data = data.reset_index().rename(columns={"Adj Close": "Close"})
    data = data[["Date", "Open", "High", "Low", "Close", "Volume"]]
    return _clean_price_dataframe(data, symbol="GC=F")


def fetch_a_share_index_or_etf(
    symbol: str, start_date: str, end_date: Optional[str] = None, is_etf: Optional[bool] = None
) -> pd.DataFrame:
    """Fetch A-share index or ETF using akshare.

    Args:
        symbol: e.g., "000001" for 上证指数 or "510300" for 沪深300ETF. Suffix (.SH/.SZ) is optional.
        start_date: YYYY-MM-DD
        end_date: YYYY-MM-DD, defaults to today
        is_etf: force ETF mode when True, index mode when False, auto-detect when None
    """

    code = symbol.replace(".SH", "").replace(".SZ", "").replace(".sh", "").replace(".sz", "")
    start = _to_datetime(start_date)
    end_dt = _to_datetime(end_date) if end_date else datetime.today()
    start_str, end_str = start.strftime("%Y%m%d"), end_dt.strftime("%Y%m%d")

    if is_etf is None:
        is_etf = code.startswith(("5", "1"))

    if is_etf:
        raw = ak.fund_etf_hist_em(
            symbol=code,
            start_date=start_str,
            end_date=end_str,
            adjust="qfq",
        )
    else:
        raw = ak.index_zh_a_hist(
            symbol=code,
            period="daily",
            start_date=start_str,
            end_date=end_str,
            adjust="",
        )

    return _clean_price_dataframe(raw, symbol=code)


def save_data(df: pd.DataFrame, filename: str) -> Path:
    """Save dataframe to the data/ directory as CSV."""

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = DATA_DIR / filename
    df.to_csv(path, index=False)
    return path