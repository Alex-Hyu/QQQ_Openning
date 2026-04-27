"""
========================================================================
  三因素早盘方向信号 - 独立 Streamlit App
  
  逻辑：基于 33 样本回测 (2026-01-15 ~ 2026-03-06)
  - 三因素同降 (ΔNE Skew<0 + ΔIV Rank<0 + ΔDR<0) → 60min 上涨概率 80%
  - 三因素同升 (ΔNE Skew>0 + ΔIV Rank>0 + ΔDR>0) → 60min 下跌概率 67%
  - 时段：t 日 09:30 → 10:25 (前 60 分钟)
  - 入场建议：09:40 之后（避开开盘混乱）
  
  使用：streamlit run app.py
  数据：上传 SpotGamma 导出的 QQQ 手动数据 CSV (按日期排列)
========================================================================
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
from dataclasses import dataclass


# ==================== 页面配置 ====================
st.set_page_config(
    page_title="三因素早盘信号",
    page_icon="🎯",
    layout="wide"
)


# ==================== 数据清洗工具 ====================

def pct_to_float(x):
    """把百分比字符串转为数值。'12.94%' -> 12.94, '$7.27' -> 7.27"""
    if pd.isna(x):
        return np.nan
    s = str(x).strip().replace('%', '').replace('$', '').replace(',', '')
    try:
        return float(s)
    except (ValueError, TypeError):
        return np.nan


def load_spotgamma_csv(source) -> pd.DataFrame:
    """读取并清洗 QQQ 手动数据 CSV"""
    if isinstance(source, pd.DataFrame):
        df = source.copy()
    else:
        df = pd.read_csv(source)
    
    # 列名标准化：替换不间断空格 \xa0
    df.columns = df.columns.str.replace('\xa0', ' ', regex=False).str.strip()
    
    # 日期解析
    df['Date'] = pd.to_datetime(df['Date'].astype(str).str.strip(), errors='coerce')
    df = df.dropna(subset=['Date']).sort_values('Date').reset_index(drop=True)
    
    # 百分比字段
    pct_cols = ['NE Skew', 'Skew', '1 M RV', '1 M IV', 'IV Rank',
                'Garch Rank', 'Skew Rank', 'Options Implied Move',
                'Next Exp Gamma', 'Next Exp Delta',
                'DPI', '%DPI Volume', '5Day DPI', '5D% DPI Volume']
    for c in pct_cols:
        if c in df.columns:
            df[c] = df[c].apply(pct_to_float)
    
    # 数值字段
    num_cols = ['Volume Ratio', 'Gamma Ratio', 'Delta Ratio', 'Put/Call OI Ratio',
                'previous close', 'Current Price(盘前价)',
                'Key Gamma Strike', 'Key Delta Strike',
                'Hedge Wall', 'Call Wall', 'Put Wall']
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')
    
    return df


# ==================== 信号数据结构 ====================

@dataclass
class ThreeFactorSignal:
    signal: str
    direction_zh: str
    confidence: str
    
    date_t1: pd.Timestamp
    date_t2: pd.Timestamp
    
    ne_skew_t1: float
    ne_skew_t2: float
    iv_rank_t1: float
    iv_rank_t2: float
    delta_ratio_t1: float
    delta_ratio_t2: float
    
    d_ne_skew: float
    d_iv_rank: float
    d_delta_ratio: float
    
    bullish_count: int
    bearish_count: int
    
    hist_win_rate: float
    hist_avg_return: float
    hist_sample_n: int
    
    note: str


# ==================== 核心信号计算 ====================

def compute_three_factor_signal(df: pd.DataFrame) -> ThreeFactorSignal:
    """从 DataFrame 取最新两行计算信号"""
    if len(df) < 2:
        raise ValueError(f"至少需要 2 行数据，当前只有 {len(df)} 行")
    
    required = ['NE Skew', 'IV Rank', 'Delta Ratio']
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"CSV 缺少必需字段: {missing}")
    
    t2 = df.iloc[-2]  # 前日
    t1 = df.iloc[-1]  # 昨日
    
    for col in required:
        if pd.isna(t1[col]) or pd.isna(t2[col]):
            raise ValueError(f"最新两行的 {col} 字段含空值，请检查 CSV")
    
    # 提取数值
    ne_t1 = float(t1['NE Skew'])
    ne_t2 = float(t2['NE Skew'])
    ivr_t1 = float(t1['IV Rank'])
    ivr_t2 = float(t2['IV Rank'])
    dr_t1 = float(t1['Delta Ratio'])
    dr_t2 = float(t2['Delta Ratio'])
    
    # 计算变化量
    d_ne = ne_t1 - ne_t2
    d_ivr = ivr_t1 - ivr_t2
    d_dr = dr_t1 - dr_t2
    
    # 严格按回测：<0 或 >0
    bullish_count = sum([d_ne < 0, d_ivr < 0, d_dr < 0])
    bearish_count = sum([d_ne > 0, d_ivr > 0, d_dr > 0])
    
    # 信号判定
    if bullish_count == 3:
        signal = "LONG"
        direction_zh = "🟢 看多 (三因素同降)"
        confidence = "HIGH"
        hist_win_rate, hist_avg_return, hist_sample_n = 80.0, 0.13, 10
        note = (
            "t-1 收盘后 NE Skew、IV Rank、Delta Ratio 三者同时下降，"
            "对应『恐慌指标全面退潮』模式 → t 日早盘倾向反弹。"
            "⚠️ 若宏观状态为『恶化中/高危震荡』则此信号失效。"
        )
    elif bearish_count == 3:
        signal = "SHORT"
        direction_zh = "🔴 看空 (三因素同升)"
        confidence = "HIGH"
        hist_win_rate, hist_avg_return, hist_sample_n = 66.7, -0.17, 9
        note = (
            "t-1 收盘后 NE Skew、IV Rank、Delta Ratio 三者同时上升，"
            "对应『风险溢价全面抬升』模式 → t 日早盘倾向走弱。"
        )
    elif bullish_count == 2:
        signal = "LONG_WEAK"
        direction_zh = "🟡 弱看多 (2/3 同降)"
        confidence = "LOW"
        hist_win_rate, hist_avg_return, hist_sample_n = 0.0, 0.0, 0
        note = "三因素未同时满足，信号强度不足，建议观望或等待开盘后 30 分钟方向确认。"
    elif bearish_count == 2:
        signal = "SHORT_WEAK"
        direction_zh = "🟡 弱看空 (2/3 同升)"
        confidence = "LOW"
        hist_win_rate, hist_avg_return, hist_sample_n = 0.0, 0.0, 0
        note = "三因素未同时满足，信号强度不足，建议观望或等待开盘后 30 分钟方向确认。"
    else:
        signal = "NEUTRAL"
        direction_zh = "⚪ 中性 (无明确信号)"
        confidence = "LOW"
        hist_win_rate, hist_avg_return, hist_sample_n = 57.6, 0.05, 33
        note = "三因素方向分歧，无明确预期。早盘策略以 SpotGamma 关键位 (PW/CW/ZG) 为主导。"
    
    return ThreeFactorSignal(
        signal=signal,
        direction_zh=direction_zh,
        confidence=confidence,
        date_t1=t1['Date'],
        date_t2=t2['Date'],
        ne_skew_t1=ne_t1, ne_skew_t2=ne_t2,
        iv_rank_t1=ivr_t1, iv_rank_t2=ivr_t2,
        delta_ratio_t1=dr_t1, delta_ratio_t2=dr_t2,
        d_ne_skew=d_ne, d_iv_rank=d_ivr, d_delta_ratio=d_dr,
        bullish_count=bullish_count,
        bearish_count=bearish_count,
        hist_win_rate=hist_win_rate,
        hist_avg_return=hist_avg_return,
        hist_sample_n=hist_sample_n,
        note=note,
    )


# ==================== Streamlit 界面 ====================

def main():
    st.title("🎯 三因素早盘方向信号")
    st.caption("基于 SpotGamma NE Skew + IV Rank + Delta Ratio 的 t-1 → t 日早盘方向预测")
    
    # ---------- 顶部说明 ----------
    with st.expander("📖 模型说明", expanded=False):
        st.markdown("""
        ### 核心逻辑
        
        基于 **2026-01-15 ~ 2026-03-06 共 33 个样本**的回测，发现三个 SpotGamma 指标的**变化量**对 t 日早盘方向有显著预测力：
        
        | 因素 | 含义 |
        |------|------|
        | **NE Skew** | 近月偏度（看跌期权 IV 相对水平） |
        | **IV Rank** | 隐含波动率分位数 |
        | **Delta Ratio** | Put Delta / Call Delta |
        
        ### 信号规则
        
        | 条件 | 信号 | 历史胜率 | 平均收益 | 样本量 |
        |------|------|---------|---------|--------|
        | 三因素同降 (Δ<0) | 🟢 **看多** | **80.0%** | +0.13% | 10 |
        | 三因素同升 (Δ>0) | 🔴 **看空** | **66.7%** | -0.17% | 9 |
        | 2/3 同向 | 🟡 弱信号 | - | - | - |
        | 0-1/3 同向 | ⚪ 中性 | 57.6% (基线) | +0.05% | 33 |
        
        ### 时段定义
        
        - **目标窗口**: t 日 **09:30 开盘 → 10:25 收盘** (前 60 分钟)
        - **基准价**: t 日 09:30 开盘价（不含隔夜 gap）
        - **入场建议**: t 日 **09:40 之后**（避开开盘混乱阶段，前 15 分钟胜率仅 50%）
        
        ### 风险提示
        
        - 33 样本量偏小，统计置信度有限
        - 看多信号在『市场结构恶化』状态下可能失效（建议结合宏观判断）
        - 收益绝对值不大（0.13%-0.17%），更适合配合 0DTE 期权放杠杆
        """)
    
    # ---------- 文件上传 ----------
    st.markdown("### 📤 上传 QQQ 手动数据 CSV")
    
    csv_file = st.file_uploader(
        "上传 SpotGamma 导出的 QQQ 手动数据（必须包含 Date / NE Skew / IV Rank / Delta Ratio 列）",
        type='csv',
        key='qqq_csv'
    )
    
    if csv_file is None:
        st.info("👆 请上传 CSV 文件以查看信号")
        with st.expander("📋 CSV 格式示例"):
            st.markdown("""
            CSV 需要包含至少以下列（列名必须完全匹配）：
            - `Date` — 日期，例如 `2026/3/6` 或 `2026-03-06`
            - `NE Skew` — 例如 `-22.86%` 或 `-22.86`
            - `IV Rank` — 例如 `23.65%` 或 `23.65`
            - `Delta Ratio` — 例如 `-0.72`
            
            数据按日期排列即可，模块会自动取最新两行作为 t-1 和 t-2。
            """)
        return
    
    # ---------- 加载数据 ----------
    try:
        df = load_spotgamma_csv(csv_file)
    except Exception as e:
        st.error(f"❌ CSV 读取失败: {e}")
        return
    
    if len(df) < 2:
        st.warning(f"⚠️ CSV 中只有 {len(df)} 行数据，至少需要 2 行才能计算变化量")
        return
    
    # ---------- 计算信号 ----------
    try:
        sig = compute_three_factor_signal(df)
    except ValueError as e:
        st.error(f"❌ 信号计算失败: {e}")
        return
    
    st.success(f"✅ 已加载 {len(df)} 行数据，日期范围 {df['Date'].min().date()} ~ {df['Date'].max().date()}")
    
    st.markdown("---")
    
    # ---------- 数据来源提示 ----------
    st.markdown(
        f"### 📅 信号基于 **{sig.date_t1.strftime('%Y-%m-%d')}** 收盘后数据 "
        f"(对比前日 {sig.date_t2.strftime('%Y-%m-%d')})"
    )
    st.caption("自动取 CSV 最新两行作为 t-1 (昨日) 和 t-2 (前日)")
    
    # ---------- 信号显示 ----------
    if sig.signal == "LONG":
        st.success(f"## {sig.direction_zh}")
    elif sig.signal == "SHORT":
        st.error(f"## {sig.direction_zh}")
    elif sig.signal in ("LONG_WEAK", "SHORT_WEAK"):
        st.warning(f"## {sig.direction_zh}")
    else:
        st.info(f"## {sig.direction_zh}")
    
    # ---------- 三因素详情表 ----------
    st.markdown("### 📊 三因素数值与变化量")
    
    factor_data = pd.DataFrame({
        '指标': ['NE Skew (%)', 'IV Rank (%)', 'Delta Ratio'],
        f't-2 ({sig.date_t2.strftime("%m-%d")})': [
            f"{sig.ne_skew_t2:.2f}",
            f"{sig.iv_rank_t2:.2f}",
            f"{sig.delta_ratio_t2:.2f}",
        ],
        f't-1 ({sig.date_t1.strftime("%m-%d")})': [
            f"{sig.ne_skew_t1:.2f}",
            f"{sig.iv_rank_t1:.2f}",
            f"{sig.delta_ratio_t1:.2f}",
        ],
        '变化量 Δ': [
            f"{sig.d_ne_skew:+.2f}",
            f"{sig.d_iv_rank:+.2f}",
            f"{sig.d_delta_ratio:+.2f}",
        ],
        '方向': [
            "🔻 降" if sig.d_ne_skew < 0 else ("🔺 升" if sig.d_ne_skew > 0 else "⏸ 平"),
            "🔻 降" if sig.d_iv_rank < 0 else ("🔺 升" if sig.d_iv_rank > 0 else "⏸ 平"),
            "🔻 降" if sig.d_delta_ratio < 0 else ("🔺 升" if sig.d_delta_ratio > 0 else "⏸ 平"),
        ],
    })
    st.dataframe(factor_data, hide_index=True, use_container_width=True)
    
    # ---------- 信号统计计数 ----------
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("看多因素数", f"{sig.bullish_count}/3", help="三因素中下降的个数")
    with col2:
        st.metric("看空因素数", f"{sig.bearish_count}/3", help="三因素中上升的个数")
    with col3:
        st.metric("置信度", sig.confidence)
    
    # ---------- 历史回测参考 ----------
    if sig.confidence == "HIGH":
        st.markdown("### 📈 历史回测参考")
        m1, m2, m3 = st.columns(3)
        m1.metric("历史胜率", f"{sig.hist_win_rate:.1f}%")
        m2.metric("平均收益", f"{sig.hist_avg_return:+.2f}%")
        m3.metric("样本量", f"N = {sig.hist_sample_n}")
    
    # ---------- 操作指引 ----------
    st.markdown("### 💡 操作指引")
    
    indication = f"""
    **目标窗口**: t 日 09:30 (开盘价) → 10:25 (前 60 分钟)
    
    **入场时段**: t 日 **09:40 之后** （避开开盘前 10 分钟混乱阶段）
    
    **解读**: {sig.note}
    """
    st.info(indication)
    
    # ---------- 历史数据预览 ----------
    with st.expander("📋 查看完整历史数据"):
        display_cols = ['Date', 'NE Skew', 'IV Rank', 'Delta Ratio']
        display_df = df[[c for c in display_cols if c in df.columns]].copy()
        if 'Date' in display_df.columns:
            display_df['Date'] = display_df['Date'].dt.strftime('%Y-%m-%d')
        st.dataframe(display_df, hide_index=True, use_container_width=True)
    
    # ---------- 历史信号回测 ----------
    with st.expander("🔍 查看 CSV 内所有日期的历史信号"):
        st.caption("把 CSV 中每一天作为 t-1，对比前一天作为 t-2，计算历史信号")
        
        if len(df) >= 3:
            history_signals = []
            for i in range(1, len(df)):
                t2_row = df.iloc[i-1]
                t1_row = df.iloc[i]
                
                if pd.isna(t1_row['NE Skew']) or pd.isna(t2_row['NE Skew']):
                    continue
                if pd.isna(t1_row['IV Rank']) or pd.isna(t2_row['IV Rank']):
                    continue
                if pd.isna(t1_row['Delta Ratio']) or pd.isna(t2_row['Delta Ratio']):
                    continue
                
                d_ne = float(t1_row['NE Skew']) - float(t2_row['NE Skew'])
                d_ivr = float(t1_row['IV Rank']) - float(t2_row['IV Rank'])
                d_dr = float(t1_row['Delta Ratio']) - float(t2_row['Delta Ratio'])
                
                bull = sum([d_ne < 0, d_ivr < 0, d_dr < 0])
                bear = sum([d_ne > 0, d_ivr > 0, d_dr > 0])
                
                if bull == 3:
                    sig_str = "🟢 看多"
                elif bear == 3:
                    sig_str = "🔴 看空"
                elif bull == 2:
                    sig_str = "🟡 弱多"
                elif bear == 2:
                    sig_str = "🟡 弱空"
                else:
                    sig_str = "⚪ 中性"
                
                history_signals.append({
                    '基准日 (t-1)': t1_row['Date'].strftime('%Y-%m-%d'),
                    'ΔNE Skew': f"{d_ne:+.2f}",
                    'ΔIV Rank': f"{d_ivr:+.2f}",
                    'ΔDR': f"{d_dr:+.2f}",
                    '信号': sig_str,
                })
            
            if history_signals:
                hist_df = pd.DataFrame(history_signals)
                # 倒序：最新的在前面
                hist_df = hist_df.iloc[::-1].reset_index(drop=True)
                st.dataframe(hist_df, hide_index=True, use_container_width=True)
                
                # 统计
                total = len(history_signals)
                long_count = sum(1 for s in history_signals if '看多' in s['信号'] and '弱' not in s['信号'])
                short_count = sum(1 for s in history_signals if '看空' in s['信号'] and '弱' not in s['信号'])
                weak_count = sum(1 for s in history_signals if '弱' in s['信号'])
                neutral_count = total - long_count - short_count - weak_count
                
                st.caption(
                    f"统计: 共 {total} 条信号 | "
                    f"🟢 看多 {long_count} | "
                    f"🔴 看空 {short_count} | "
                    f"🟡 弱信号 {weak_count} | "
                    f"⚪ 中性 {neutral_count}"
                )
        else:
            st.info("数据少于 3 行，无法回看历史信号")
    
    # ---------- 页脚 ----------
    st.markdown("---")
    st.caption(
        f"📅 当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
        f"🎯 三因素早盘信号 v1.0 | "
        f"基于 33 样本回测 (2026-01-15 ~ 2026-03-06)"
    )


if __name__ == "__main__":
    main()
