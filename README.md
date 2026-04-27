# 🎯 三因素早盘方向信号

基于 SpotGamma **NE Skew + IV Rank + Delta Ratio** 三因素变化量的 t 日早盘方向预测系统。

## 核心逻辑

| 信号 | 条件 | 历史胜率 | 平均收益 |
|------|------|---------|---------|
| 🟢 看多 | 三因素同降 (Δ<0) | **80.0%** (10/13) | +0.13% |
| 🔴 看空 | 三因素同升 (Δ>0) | **66.7%** (6/9) | -0.17% |

**目标窗口**: t 日 09:30 → 10:25 (前 60 分钟)
**入场时段**: 09:40 之后（避开开盘混乱阶段）

## 部署到 Streamlit Cloud

### 1. 创建 GitHub 仓库

```bash
# 创建一个新仓库，把这三个文件传上去
- app.py
- requirements.txt
- README.md
```

### 2. 部署到 Streamlit Cloud

1. 访问 [share.streamlit.io](https://share.streamlit.io)
2. 点击 "New app"
3. 选择你的 GitHub 仓库
4. 主文件填 `app.py`
5. 点击 Deploy

部署完成后会得到一个公开 URL（例如 `https://yourapp.streamlit.app`）。

## 本地运行

```bash
pip install -r requirements.txt
streamlit run app.py
```

## CSV 数据格式

CSV 必须包含以下列（列名严格匹配）：

| 列名 | 示例值 | 说明 |
|------|--------|------|
| `Date` | `2026/3/6` 或 `2026-03-06` | 交易日 |
| `NE Skew` | `-22.86%` 或 `-22.86` | 近月偏度 |
| `IV Rank` | `23.65%` 或 `23.65` | 隐含波动率分位 |
| `Delta Ratio` | `-0.72` | Put Delta / Call Delta |

数据按日期排列即可。模块自动取**最新两行**作为 t-1 (昨日) 和 t-2 (前日)，计算变化量。

## 使用方法

1. 每个交易日收盘后，从 SpotGamma 导出新一行数据，追加到 CSV 末尾
2. 上传更新后的 CSV
3. 系统自动用最新两行计算次日早盘信号

## 风险提示

- 33 样本量偏小，统计置信度有限
- 看多信号在『市场结构恶化』状态下可能失效
- 收益绝对值不大（0.13%-0.17%），更适合配合 SPXW/QQQ 0DTE 期权放杠杆
