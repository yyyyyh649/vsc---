# 大类资产轮动研究框架（黄金期货 vs A股指数/ETF）

## 项目简介 (Project Overview)
本项目用于研究与验证黄金期货（GC=F）与A股指数/ETF之间的轮动规律，当前聚焦策略验证，不涉及实盘交易。框架包含数据获取、动量轮动策略、回测验证及未来实盘适配接口占位。

## 环境依赖 (Installation)
1. 创建并激活虚拟环境（示例：Windows PowerShell）：
   ```powershell
   python -m venv .venv
   .\.venv\Scripts\activate
   ```
2. 安装依赖：
   ```powershell
   pip install -r requirements.txt
   ```

## 快速开始 (Quick Start)
运行示例回测脚本（默认标的：黄金期货 GC=F 与 510300ETF，起始 2015-01-01）：
```powershell
python backtests/validate_strategy.py
```
运行后会在 data/ 目录生成回测 CSV，并在终端打印绩效指标。

## 策略逻辑 (Strategy Logic)
- 核心：使用固定窗口（默认 60 日）动量择时，在黄金与A股资产间轮动；若两者动量均为负，持有现金位。
- 调仓频率：日/周/月底可选（默认周五），换仓扣除单边费率（默认 5 bps）。
- 未来函数处理：信号在收盘后生成，**在回测执行时需滞后一日生效**，避免使用当日价格做当日决策的“未来函数”问题；请在实际策略运行时确保使用前一交易日的信号执行当日交易。

## 数据说明 (Data Sources)
- 黄金期货：Yahoo Finance (GC=F)。
- A股指数/ETF：Akshare（`index_zh_a_hist`、`fund_etf_hist_em`）。
- 网络注意事项：
  - 中国大陆直连 Yahoo 可能不稳定，必要时配置代理或改用本地数据源。
  - 中美市场时区与节假日不同，请在研究时留意数据对齐与补全方式。

## 项目结构 (File Structure)
- `src/data_fetcher.py`：黄金与A股数据抓取与标准化、CSV 保存。
- `strategies/rotation_strategy.py`：动量轮动信号与绩效指标计算。
- `backtests/validate_strategy.py`：示例回测入口，下载数据并输出曲线与指标。
- `adapters/variety_adapter.py`：实盘/模拟交易适配器接口占位与 Dummy 实现。
- `requirements.txt`：依赖清单。

## 免责声明 (Disclaimer)
本项目仅用于研究与教学，不构成任何投资建议。历史回测不代表未来表现，金融市场具有高度不确定性，使用者需自行承担全部风险。实盘交易前请充分评估策略有效性、流动性、滑点、成本及合规要求。
