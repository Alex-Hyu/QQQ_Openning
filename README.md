# 🎯 三因素早盘方向信号 v2

基于 SpotGamma **NE Skew + IV Rank + Delta Ratio** 三因素变化量的 t 日早盘方向预测系统，**含 Google Sheets 持久化 + 自动验证胜率**功能。

## 核心功能

✅ 上传 SpotGamma CSV → 自动计算次日早盘信号  
✅ 信号自动保存到 Google Sheets（云端持久化，重启不丢失）  
✅ 打开 app 自动验证所有过去日期的胜率  
✅ 验证三个时段：30min / 60min / 收盘  
✅ 累积胜率统计（LONG / SHORT / 总体）

## 信号规则

| 条件 | 信号 | 历史胜率 (60min) |
|------|------|---------|
| 三因素同降 (Δ<0) | 🟢 看多 | 80.0% (N=10) |
| 三因素同升 (Δ>0) | 🔴 看空 | 66.7% (N=9) |
| 2/3 同向 | 🟡 弱信号 | - |
| 0-1/3 同向 | ⚪ 中性 | 57.6% (基线) |

## 部署到 Streamlit Cloud

### 第一步：创建 GitHub 仓库

1. 创建一个新的公开仓库
2. 上传以下三个文件：
   - `app.py`
   - `requirements.txt`
   - `README.md`

### 第二步：部署到 Streamlit Cloud

1. 访问 [share.streamlit.io](https://share.streamlit.io)
2. New app → 选择仓库 → Main file: `app.py` → Deploy

### 第三步：配置 Google Sheets（关键）

参见 app 内 "如何配置 Google Sheets" 折叠面板，5 步完成：

1. **创建 Google Cloud 项目**，启用 Google Sheets API + Google Drive API
2. **创建 Service Account**，下载 JSON 凭证
3. **创建 Google Sheets**，命名为 `ThreeFactor_Signals`，把 service account 邮箱加为 Editor
4. **在 Streamlit Cloud → Settings → Secrets** 中配置：

```toml
[gcp_service_account]
type = "service_account"
project_id = "你的项目ID"
private_key_id = "..."
private_key = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
client_email = "xxx@xxx.iam.gserviceaccount.com"
client_id = "..."
auth_uri = "https://accounts.google.com/o/oauth2/auth"
token_uri = "https://oauth2.googleapis.com/token"
auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
client_x509_cert_url = "..."
```

5. **重启 app**，看到 ☁️ Google Sheets 已连接 就好了

## 使用流程

### 每日操作（30 秒）

1. 收盘后从 SpotGamma 复制新一行数据，追加到本地 CSV 末尾
2. 打开 app（自动验证昨天的信号）
3. 上传更新后的 CSV → 看到次日早盘信号
4. 点 **保存到 Google Sheets** → 永久记录

### 验证机制

- **自动触发**：每次打开 app，系统自动检查 Google Sheets 中所有 `target_date <= 今天` 但 `verified_eod` 为空的记录
- **数据来源**：从 yfinance 拉取 QQQ 5min K 线
- **判定标准**：
  - `verified_30m`: t 日 09:30 → 09:55 涨跌方向是否符合信号
  - `verified_60m`: t 日 09:30 → 10:25 涨跌方向是否符合信号
  - `verified_eod`: t 日 09:30 → 收盘 涨跌方向是否符合信号

### Google Sheets 数据结构

| 字段 | 说明 |
|------|------|
| signal_date | t-1 日（信号产生日） |
| target_date | t 日（信号目标日） |
| signal | LONG / SHORT / LONG_WEAK / SHORT_WEAK / NEUTRAL |
| direction | 中文方向标签 |
| d_ne_skew, d_iv_rank, d_delta_ratio | 三因素变化量 |
| open_930, close_955, close_1025, close_eod | t 日各时点价格 |
| ret_30m, ret_60m, ret_eod | 各时段收益率 |
| verified_30m, verified_60m, verified_eod | 方向是否正确 (True/False) |
| verified_at | 验证时间戳 |

## 风险提示

- yfinance 5min 数据只能拉最近 60 天，超过 60 天的历史日期无法验证
- 33 样本量偏小，统计置信度有限
- 看多信号在『市场结构恶化』状态下可能失效
- 收益绝对值不大（0.13%-0.17%），更适合配合 0DTE 期权放杠杆

## 本地运行

```bash
pip install -r requirements.txt
streamlit run app.py
```

本地运行时 Google Sheets 默认不连接（除非配置 `~/.streamlit/secrets.toml`），但所有信号计算和单次显示功能都正常。
