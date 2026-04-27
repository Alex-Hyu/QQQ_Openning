"""
========================================================================
  三因素早盘方向信号 - V2 含自动验证 + Google Sheets 持久化
  
  功能：
  1. 上传 SpotGamma CSV → 自动计算 t 日早盘信号
  2. 信号自动保存到 Google Sheets
  3. 打开 app 自动验证所有过去日期的胜率（30min/60min/收盘）
  4. 显示累积胜率统计
  
  使用：streamlit run app.py
========================================================================
"""

import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
import json
from datetime import datetime, timedelta, time
from dataclasses import dataclass, asdict
import pytz


# ==================== 页面配置 ====================
st.set_page_config(
    page_title="三因素早盘信号 v2",
    page_icon="🎯",
    layout="wide"
)


# ==================== Google Sheets 配置 ====================
GSHEETS_AVAILABLE = False
try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSHEETS_AVAILABLE = True
except ImportError:
    pass

GSHEETS_SPREADSHEET_NAME = "ThreeFactor_Signals"
GSHEETS_WORKSHEET_NAME = "signals_log"


def get_gsheets_client():
    """获取 Google Sheets 客户端"""
    if not GSHEETS_AVAILABLE:
        return None
    try:
        if hasattr(st, 'secrets') and 'gcp_service_account' in st.secrets:
            creds = Credentials.from_service_account_info(
                st.secrets['gcp_service_account'],
                scopes=[
                    'https://www.googleapis.com/auth/spreadsheets',
                    'https://www.googleapis.com/auth/drive'
                ]
            )
            return gspread.authorize(creds)
    except Exception as e:
        return None
    return None


def load_signals_from_gsheets():
    """从 Google Sheets 读取所有历史信号"""
    client = get_gsheets_client()
    if not client:
        return None
    
    try:
        try:
            spreadsheet = client.open(GSHEETS_SPREADSHEET_NAME)
        except gspread.exceptions.SpreadsheetNotFound:
            return None
        
        try:
            ws = spreadsheet.worksheet(GSHEETS_WORKSHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(title=GSHEETS_WORKSHEET_NAME, rows=1000, cols=20)
            ws.append_row([
                'signal_date', 'target_date', 'signal', 'direction',
                'd_ne_skew', 'd_iv_rank', 'd_delta_ratio',
                'open_930', 'close_955', 'close_1025', 'close_eod',
                'ret_30m', 'ret_60m', 'ret_eod',
                'verified_30m', 'verified_60m', 'verified_eod',
                'verified_at'
            ])
            return []
        
        all_values = ws.get_all_values()
        if len(all_values) <= 1:
            return []
        
        headers = all_values[0]
        records = []
        for row in all_values[1:]:
            if len(row) >= len(headers):
                record = dict(zip(headers, row))
                records.append(record)
        return records
    except Exception as e:
        st.warning(f"Google Sheets 读取失败: {e}")
        return None


def save_signal_to_gsheets(signal_record):
    """新增/更新一条信号记录到 Google Sheets"""
    client = get_gsheets_client()
    if not client:
        return False
    
    try:
        spreadsheet = client.open(GSHEETS_SPREADSHEET_NAME)
        try:
            ws = spreadsheet.worksheet(GSHEETS_WORKSHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(title=GSHEETS_WORKSHEET_NAME, rows=1000, cols=20)
            ws.append_row([
                'signal_date', 'target_date', 'signal', 'direction',
                'd_ne_skew', 'd_iv_rank', 'd_delta_ratio',
                'open_930', 'close_955', 'close_1025', 'close_eod',
                'ret_30m', 'ret_60m', 'ret_eod',
                'verified_30m', 'verified_60m', 'verified_eod',
                'verified_at'
            ])
        
        all_values = ws.get_all_values()
        target_date = signal_record['target_date']
        
        # 查找是否已存在
        existing_row = None
        for idx, row in enumerate(all_values[1:], start=2):
            if len(row) >= 2 and row[1] == target_date:
                existing_row = idx
                break
        
        new_row = [
            signal_record.get('signal_date', ''),
            signal_record.get('target_date', ''),
            signal_record.get('signal', ''),
            signal_record.get('direction', ''),
            str(signal_record.get('d_ne_skew', '')),
            str(signal_record.get('d_iv_rank', '')),
            str(signal_record.get('d_delta_ratio', '')),
            str(signal_record.get('open_930', '')),
            str(signal_record.get('close_955', '')),
            str(signal_record.get('close_1025', '')),
            str(signal_record.get('close_eod', '')),
            str(signal_record.get('ret_30m', '')),
            str(signal_record.get('ret_60m', '')),
            str(signal_record.get('ret_eod', '')),
            str(signal_record.get('verified_30m', '')),
            str(signal_record.get('verified_60m', '')),
            str(signal_record.get('verified_eod', '')),
            str(signal_record.get('verified_at', '')),
        ]
        
        if existing_row:
            ws.update(f'A{existing_row}:R{existing_row}', [new_row])
        else:
            ws.append_row(new_row)
        
        return True
    except Exception as e:
        st.warning(f"Google Sheets 保存失败: {e}")
        return False


def update_verification_in_gsheets(target_date, verification_data):
    """只更新某条记录的验证字段"""
    client = get_gsheets_client()
    if not client:
        return False
    
    try:
        spreadsheet = client.open(GSHEETS_SPREADSHEET_NAME)
        ws = spreadsheet.worksheet(GSHEETS_WORKSHEET_NAME)
        all_values = ws.get_all_values()
        
        for idx, row in enumerate(all_values[1:], start=2):
            if len(row) >= 2 and row[1] == target_date:
                # 列 H-R (8-18) 是验证字段
                update_row = [
                    str(verification_data.get('open_930', '')),
                    str(verification_data.get('close_955', '')),
                    str(verification_data.get('close_1025', '')),
                    str(verification_data.get('close_eod', '')),
                    str(verification_data.get('ret_30m', '')),
                    str(verification_data.get('ret_60m', '')),
                    str(verification_data.get('ret_eod', '')),
                    str(verification_data.get('verified_30m', '')),
                    str(verification_data.get('verified_60m', '')),
                    str(verification_data.get('verified_eod', '')),
                    str(verification_data.get('verified_at', '')),
                ]
                ws.update(f'H{idx}:R{idx}', [update_row])
                return True
        return False
    except Exception as e:
        return False


# ==================== 数据清洗工具 ====================

def pct_to_float(x):
    if pd.isna(x):
        return np.nan
    s = str(x).strip().replace('%', '').replace('$', '').replace(',', '')
    try:
        return float(s)
    except (ValueError, TypeError):
        return np.nan


def load_spotgamma_csv(source) -> pd.DataFrame:
    if isinstance(source, pd.DataFrame):
        df = source.copy()
    else:
        df = pd.read_csv(source)
    
    df.columns = df.columns.str.replace('\xa0', ' ', regex=False).str.strip()
    df['Date'] = pd.to_datetime(df['Date'].astype(str).str.strip(), errors='coerce')
    df = df.dropna(subset=['Date']).sort_values('Date').reset_index(drop=True)
    
    pct_cols = ['NE Skew', 'Skew', '1 M RV', '1 M IV', 'IV Rank',
                'Garch Rank', 'Skew Rank', 'Options Implied Move',
                'Next Exp Gamma', 'Next Exp Delta',
                'DPI', '%DPI Volume', '5Day DPI', '5D% DPI Volume']
    for c in pct_cols:
        if c in df.columns:
            df[c] = df[c].apply(pct_to_float)
    
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


def build_signal_from_changes(date_t1, date_t2, ne_t1, ne_t2, ivr_t1, ivr_t2, dr_t1, dr_t2):
    """从两天数据构建信号对象"""
    d_ne = ne_t1 - ne_t2
    d_ivr = ivr_t1 - ivr_t2
    d_dr = dr_t1 - dr_t2
    
    bullish_count = sum([d_ne < 0, d_ivr < 0, d_dr < 0])
    bearish_count = sum([d_ne > 0, d_ivr > 0, d_dr > 0])
    
    if bullish_count == 3:
        signal, direction_zh, confidence = "LONG", "🟢 看多 (三因素同降)", "HIGH"
        hist_win_rate, hist_avg_return, hist_sample_n = 80.0, 0.13, 10
        note = "三因素同时下降 → 恐慌指标全面退潮 → t 日早盘倾向反弹"
    elif bearish_count == 3:
        signal, direction_zh, confidence = "SHORT", "🔴 看空 (三因素同升)", "HIGH"
        hist_win_rate, hist_avg_return, hist_sample_n = 66.7, -0.17, 9
        note = "三因素同时上升 → 风险溢价全面抬升 → t 日早盘倾向走弱"
    elif bullish_count == 2:
        signal, direction_zh, confidence = "LONG_WEAK", "🟡 弱看多 (2/3 同降)", "LOW"
        hist_win_rate, hist_avg_return, hist_sample_n = 0.0, 0.0, 0
        note = "三因素未同时满足，信号强度不足"
    elif bearish_count == 2:
        signal, direction_zh, confidence = "SHORT_WEAK", "🟡 弱看空 (2/3 同升)", "LOW"
        hist_win_rate, hist_avg_return, hist_sample_n = 0.0, 0.0, 0
        note = "三因素未同时满足，信号强度不足"
    else:
        signal, direction_zh, confidence = "NEUTRAL", "⚪ 中性 (无明确信号)", "LOW"
        hist_win_rate, hist_avg_return, hist_sample_n = 57.6, 0.05, 33
        note = "三因素方向分歧，无明确预期"
    
    return ThreeFactorSignal(
        signal=signal, direction_zh=direction_zh, confidence=confidence,
        date_t1=date_t1, date_t2=date_t2,
        ne_skew_t1=ne_t1, ne_skew_t2=ne_t2,
        iv_rank_t1=ivr_t1, iv_rank_t2=ivr_t2,
        delta_ratio_t1=dr_t1, delta_ratio_t2=dr_t2,
        d_ne_skew=d_ne, d_iv_rank=d_ivr, d_delta_ratio=d_dr,
        bullish_count=bullish_count, bearish_count=bearish_count,
        hist_win_rate=hist_win_rate, hist_avg_return=hist_avg_return,
        hist_sample_n=hist_sample_n, note=note,
    )


def compute_three_factor_signal(df: pd.DataFrame) -> ThreeFactorSignal:
    if len(df) < 2:
        raise ValueError(f"至少需要 2 行数据")
    
    required = ['NE Skew', 'IV Rank', 'Delta Ratio']
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"CSV 缺少字段: {missing}")
    
    t2 = df.iloc[-2]
    t1 = df.iloc[-1]
    
    for col in required:
        if pd.isna(t1[col]) or pd.isna(t2[col]):
            raise ValueError(f"最新两行的 {col} 字段含空值")
    
    return build_signal_from_changes(
        t1['Date'], t2['Date'],
        float(t1['NE Skew']), float(t2['NE Skew']),
        float(t1['IV Rank']), float(t2['IV Rank']),
        float(t1['Delta Ratio']), float(t2['Delta Ratio']),
    )


# ==================== 验证逻辑 ====================

def get_next_trading_day(t1_date):
    """t-1 日 → 下一个交易日（跳过周末）"""
    if isinstance(t1_date, str):
        t1_date = datetime.strptime(t1_date, '%Y-%m-%d').date()
    elif hasattr(t1_date, 'date'):
        t1_date = t1_date.date()
    
    next_d = t1_date + timedelta(days=1)
    while next_d.weekday() >= 5:  # 5=Sat, 6=Sun
        next_d += timedelta(days=1)
    return next_d


@st.cache_data(ttl=300)  # 缓存 5 分钟
def fetch_qqq_intraday(target_date_str):
    """从 yfinance 获取 QQQ 在 target_date 的 5min K 线"""
    try:
        target_date = datetime.strptime(target_date_str, '%Y-%m-%d').date()
        # yfinance 的 5min 数据只能拉最近 60 天
        days_ago = (datetime.now().date() - target_date).days
        if days_ago > 58:
            return None, "日期超过 60 天前，yfinance 5min 数据不可用"
        if days_ago < 0:
            return None, "未来日期，无法验证"
        
        ticker = yf.Ticker("QQQ")
        # 抓取目标日 ± 1 天的范围
        start = target_date - timedelta(days=1)
        end = target_date + timedelta(days=2)
        
        df = ticker.history(
            start=start.strftime('%Y-%m-%d'),
            end=end.strftime('%Y-%m-%d'),
            interval='5m',
            prepost=False
        )
        
        if df.empty:
            return None, "yfinance 返回空数据"
        
        # 过滤到 target_date 当日
        df = df.tz_convert('America/New_York') if df.index.tz else df.tz_localize('America/New_York')
        df_day = df[df.index.date == target_date]
        
        if df_day.empty:
            return None, f"目标日 {target_date} 无数据（可能非交易日或数据未更新）"
        
        return df_day, None
    except Exception as e:
        return None, f"获取数据失败: {e}"


def verify_signal_for_date(target_date_str, signal_direction):
    """
    获取 target_date 的早盘走势，验证 30min / 60min / 收盘 的方向是否正确。
    返回 dict: open_930, close_955, close_1025, close_eod, ret_30m, ret_60m, ret_eod,
              verified_30m, verified_60m, verified_eod
    """
    df_day, err = fetch_qqq_intraday(target_date_str)
    if err:
        return None, err
    
    # 找开盘价 (09:30 的那根 5min 的 open)
    open_bar = df_day[df_day.index.strftime('%H:%M') == '09:30']
    if open_bar.empty:
        return None, "找不到 09:30 K 线"
    open_930 = float(open_bar['Open'].iloc[0])
    
    # 09:55 收盘 = 30min（前 30 分钟末）
    close_955_bar = df_day[df_day.index.strftime('%H:%M') == '09:55']
    close_955 = float(close_955_bar['Close'].iloc[0]) if not close_955_bar.empty else None
    
    # 10:25 收盘 = 60min（前 60 分钟末）
    close_1025_bar = df_day[df_day.index.strftime('%H:%M') == '10:25']
    close_1025 = float(close_1025_bar['Close'].iloc[0]) if not close_1025_bar.empty else None
    
    # 当日收盘价
    close_eod = float(df_day['Close'].iloc[-1])
    
    # 计算收益率
    ret_30m = (close_955 - open_930) / open_930 * 100 if close_955 else None
    ret_60m = (close_1025 - open_930) / open_930 * 100 if close_1025 else None
    ret_eod = (close_eod - open_930) / open_930 * 100
    
    # 判断方向是否正确
    def check_direction(ret, direction):
        if ret is None:
            return None
        if direction == "LONG":
            return ret > 0
        elif direction == "SHORT":
            return ret < 0
        else:
            return None  # 弱信号/中性不判定
    
    verified_30m = check_direction(ret_30m, signal_direction)
    verified_60m = check_direction(ret_60m, signal_direction)
    verified_eod = check_direction(ret_eod, signal_direction)
    
    return {
        'open_930': round(open_930, 2),
        'close_955': round(close_955, 2) if close_955 else None,
        'close_1025': round(close_1025, 2) if close_1025 else None,
        'close_eod': round(close_eod, 2),
        'ret_30m': round(ret_30m, 3) if ret_30m else None,
        'ret_60m': round(ret_60m, 3) if ret_60m else None,
        'ret_eod': round(ret_eod, 3),
        'verified_30m': verified_30m,
        'verified_60m': verified_60m,
        'verified_eod': verified_eod,
        'verified_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }, None


def auto_verify_pending_signals(records):
    """
    自动验证所有未完成验证的记录。
    判断条件：target_date <= 今天 AND verified_eod 为空 (或为 None/'')
    """
    et_tz = pytz.timezone('America/New_York')
    today_et = datetime.now(et_tz).date()
    
    pending = []
    for rec in records:
        target_date_str = rec.get('target_date', '')
        if not target_date_str:
            continue
        
        try:
            target_date = datetime.strptime(target_date_str, '%Y-%m-%d').date()
        except:
            continue
        
        # 必须是过去日期或今天（只要美东时间已经过 11:00 就可以验证早盘）
        if target_date > today_et:
            continue
        
        # 已经完整验证过的跳过
        v_eod = rec.get('verified_eod', '')
        if v_eod not in ['', 'None', None]:
            continue
        
        # 信号是 LONG 或 SHORT 才需要验证（中性/弱信号不验证）
        sig = rec.get('signal', '')
        if sig not in ['LONG', 'SHORT']:
            continue
        
        pending.append(rec)
    
    return pending


# ==================== Streamlit 主界面 ====================

def main():
    st.title("🎯 三因素早盘方向信号 v2")
    st.caption("含 Google Sheets 自动记录 + 自动验证胜率")
    
    # ---------- Google Sheets 状态 ----------
    gs_client = get_gsheets_client()
    if gs_client is None:
        st.error("⚠️ Google Sheets 未连接 — 信号无法持久化保存")
        with st.expander("📖 如何配置 Google Sheets", expanded=False):
            st.markdown(get_setup_guide())
    else:
        st.success("☁️ Google Sheets 已连接 — 信号会自动保存到云端")
    
    # ---------- 顶部说明 ----------
    with st.expander("📖 模型说明", expanded=False):
        st.markdown("""
        ### 信号规则
        | 条件 | 信号 | 历史胜率 | 平均收益 |
        |------|------|---------|---------|
        | 三因素同降 (Δ<0) | 🟢 **看多** | 80.0% | +0.13% |
        | 三因素同升 (Δ>0) | 🔴 **看空** | 66.7% | -0.17% |
        | 2/3 同向 | 🟡 弱信号 | - | - |
        | 0-1/3 同向 | ⚪ 中性 | 57.6% | +0.05% |
        
        ### 时段定义
        - **基准价**: t 日 09:30 开盘价
        - **30min**: 09:55 收盘
        - **60min**: 10:25 收盘
        - **收盘**: 当日 16:00 收盘
        - **入场建议**: 09:40 之后（避开开盘混乱）
        """)
    
    # ============================================================
    # 自动验证模块（打开 app 立即触发）
    # ============================================================
    if gs_client is not None:
        with st.spinner("🔄 自动验证未完成的历史信号..."):
            existing_records = load_signals_from_gsheets() or []
            pending = auto_verify_pending_signals(existing_records)
            
            if pending:
                verified_count = 0
                failed_count = 0
                for rec in pending:
                    target_date = rec['target_date']
                    direction = rec.get('signal', '')
                    
                    result, err = verify_signal_for_date(target_date, direction)
                    if result is None:
                        failed_count += 1
                        continue
                    
                    update_verification_in_gsheets(target_date, result)
                    verified_count += 1
                
                if verified_count > 0:
                    st.success(f"✅ 自动验证完成：成功 {verified_count} 条 | 失败 {failed_count} 条")
                    # 重新加载
                    existing_records = load_signals_from_gsheets() or []
            else:
                if existing_records:
                    st.caption(f"📋 历史记录共 {len(existing_records)} 条，全部已验证")
    else:
        existing_records = []
    
    # ============================================================
    # 上传 CSV → 计算今日信号
    # ============================================================
    st.markdown("---")
    st.markdown("### 📤 上传今日 SpotGamma 数据 CSV")
    
    csv_file = st.file_uploader(
        "上传 QQQ 手动数据（必须含 Date / NE Skew / IV Rank / Delta Ratio 列）",
        type='csv',
        key='qqq_csv'
    )
    
    if csv_file is not None:
        try:
            df = load_spotgamma_csv(csv_file)
        except Exception as e:
            st.error(f"❌ CSV 读取失败: {e}")
            return
        
        if len(df) < 2:
            st.warning(f"⚠️ CSV 只有 {len(df)} 行，至少需要 2 行")
            return
        
        try:
            sig = compute_three_factor_signal(df)
        except ValueError as e:
            st.error(f"❌ 信号计算失败: {e}")
            return
        
        st.success(f"✅ 已加载 {len(df)} 行数据 | 日期 {df['Date'].min().date()} ~ {df['Date'].max().date()}")
        
        # 显示当前信号
        target_date = get_next_trading_day(sig.date_t1)
        target_date_str = target_date.strftime('%Y-%m-%d')
        
        st.markdown(f"### 📅 {sig.date_t1.strftime('%Y-%m-%d')} 收盘后信号 → 预测 **{target_date_str}** 早盘")
        
        if sig.signal == "LONG":
            st.success(f"## {sig.direction_zh}")
        elif sig.signal == "SHORT":
            st.error(f"## {sig.direction_zh}")
        elif sig.signal in ("LONG_WEAK", "SHORT_WEAK"):
            st.warning(f"## {sig.direction_zh}")
        else:
            st.info(f"## {sig.direction_zh}")
        
        # 三因素详情
        factor_data = pd.DataFrame({
            '指标': ['NE Skew (%)', 'IV Rank (%)', 'Delta Ratio'],
            f't-2 ({sig.date_t2.strftime("%m-%d")})': [
                f"{sig.ne_skew_t2:.2f}", f"{sig.iv_rank_t2:.2f}", f"{sig.delta_ratio_t2:.2f}",
            ],
            f't-1 ({sig.date_t1.strftime("%m-%d")})': [
                f"{sig.ne_skew_t1:.2f}", f"{sig.iv_rank_t1:.2f}", f"{sig.delta_ratio_t1:.2f}",
            ],
            '变化量 Δ': [
                f"{sig.d_ne_skew:+.2f}", f"{sig.d_iv_rank:+.2f}", f"{sig.d_delta_ratio:+.2f}",
            ],
            '方向': [
                "🔻" if sig.d_ne_skew < 0 else ("🔺" if sig.d_ne_skew > 0 else "⏸"),
                "🔻" if sig.d_iv_rank < 0 else ("🔺" if sig.d_iv_rank > 0 else "⏸"),
                "🔻" if sig.d_delta_ratio < 0 else ("🔺" if sig.d_delta_ratio > 0 else "⏸"),
            ],
        })
        st.dataframe(factor_data, hide_index=True, use_container_width=True)
        
        # 保存到 Google Sheets
        if gs_client is not None:
            existing_target_dates = [r.get('target_date', '') for r in existing_records]
            already_saved = target_date_str in existing_target_dates
            
            col_save1, col_save2 = st.columns([1, 3])
            with col_save1:
                save_btn = st.button(
                    "🔄 更新此信号" if already_saved else "💾 保存到 Google Sheets",
                    type="primary",
                    use_container_width=True
                )
            with col_save2:
                if already_saved:
                    st.info(f"📋 {target_date_str} 已存在记录（点击按钮可覆盖更新）")
            
            if save_btn:
                signal_record = {
                    'signal_date': sig.date_t1.strftime('%Y-%m-%d'),
                    'target_date': target_date_str,
                    'signal': sig.signal,
                    'direction': sig.direction_zh,
                    'd_ne_skew': round(sig.d_ne_skew, 3),
                    'd_iv_rank': round(sig.d_iv_rank, 3),
                    'd_delta_ratio': round(sig.d_delta_ratio, 3),
                }
                if save_signal_to_gsheets(signal_record):
                    st.success(f"✅ 已保存 {target_date_str} 信号到 Google Sheets")
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error("❌ 保存失败")
        
        st.info(f"💡 {sig.note}")
    
    # ============================================================
    # 历史信号记录 + 胜率统计
    # ============================================================
    if gs_client is not None and existing_records:
        st.markdown("---")
        st.markdown("### 📊 历史信号记录与胜率统计")
        
        # 按 target_date 倒序
        sorted_records = sorted(existing_records, key=lambda r: r.get('target_date', ''), reverse=True)
        
        # ---------- 胜率统计 ----------
        def calc_accuracy(records, signal_type, time_field):
            """计算某种信号类型在某个时段的胜率"""
            relevant = [r for r in records 
                       if r.get('signal') == signal_type 
                       and r.get(time_field) in ('True', 'False')]
            if not relevant:
                return None, 0
            correct = sum(1 for r in relevant if r.get(time_field) == 'True')
            return correct / len(relevant) * 100, len(relevant)
        
        st.markdown("#### 🎯 累积胜率")
        
        # LONG 信号统计
        long_30, n_long_30 = calc_accuracy(existing_records, 'LONG', 'verified_30m')
        long_60, n_long_60 = calc_accuracy(existing_records, 'LONG', 'verified_60m')
        long_eod, n_long_eod = calc_accuracy(existing_records, 'LONG', 'verified_eod')
        
        # SHORT 信号统计
        short_30, n_short_30 = calc_accuracy(existing_records, 'SHORT', 'verified_30m')
        short_60, n_short_60 = calc_accuracy(existing_records, 'SHORT', 'verified_60m')
        short_eod, n_short_eod = calc_accuracy(existing_records, 'SHORT', 'verified_eod')
        
        # 总体（LONG + SHORT 都算入）
        all_30 = [r for r in existing_records if r.get('signal') in ('LONG', 'SHORT') and r.get('verified_30m') in ('True', 'False')]
        overall_30 = sum(1 for r in all_30 if r.get('verified_30m') == 'True') / len(all_30) * 100 if all_30 else None
        
        all_60 = [r for r in existing_records if r.get('signal') in ('LONG', 'SHORT') and r.get('verified_60m') in ('True', 'False')]
        overall_60 = sum(1 for r in all_60 if r.get('verified_60m') == 'True') / len(all_60) * 100 if all_60 else None
        
        all_eod = [r for r in existing_records if r.get('signal') in ('LONG', 'SHORT') and r.get('verified_eod') in ('True', 'False')]
        overall_eod = sum(1 for r in all_eod if r.get('verified_eod') == 'True') / len(all_eod) * 100 if all_eod else None
        
        stats_table = pd.DataFrame({
            '信号类型': ['🟢 LONG (看多)', '🔴 SHORT (看空)', '📊 总体'],
            '30min 胜率': [
                f"{long_30:.1f}% ({n_long_30}次)" if long_30 is not None else "-",
                f"{short_30:.1f}% ({n_short_30}次)" if short_30 is not None else "-",
                f"{overall_30:.1f}% ({len(all_30)}次)" if overall_30 is not None else "-",
            ],
            '60min 胜率': [
                f"{long_60:.1f}% ({n_long_60}次)" if long_60 is not None else "-",
                f"{short_60:.1f}% ({n_short_60}次)" if short_60 is not None else "-",
                f"{overall_60:.1f}% ({len(all_60)}次)" if overall_60 is not None else "-",
            ],
            '收盘 胜率': [
                f"{long_eod:.1f}% ({n_long_eod}次)" if long_eod is not None else "-",
                f"{short_eod:.1f}% ({n_short_eod}次)" if short_eod is not None else "-",
                f"{overall_eod:.1f}% ({len(all_eod)}次)" if overall_eod is not None else "-",
            ],
        })
        st.dataframe(stats_table, hide_index=True, use_container_width=True)
        
        # 与回测基准对比
        st.caption(
            "📌 回测基准（33 样本，2026-01-15 ~ 2026-03-06）："
            "LONG 60min 胜率 80% (N=10) | SHORT 60min 胜率 67% (N=9)"
        )
        
        # ---------- 详细记录表 ----------
        st.markdown("#### 📋 全部信号记录")
        
        display_rows = []
        for rec in sorted_records:
            sig_type = rec.get('signal', '')
            
            # 信号图标
            if sig_type == 'LONG':
                sig_icon = "🟢"
            elif sig_type == 'SHORT':
                sig_icon = "🔴"
            elif sig_type in ('LONG_WEAK', 'SHORT_WEAK'):
                sig_icon = "🟡"
            else:
                sig_icon = "⚪"
            
            # 验证状态
            def verify_icon(val):
                if val == 'True':
                    return "✅"
                elif val == 'False':
                    return "❌"
                else:
                    return "⏳"
            
            # 收益率
            def fmt_ret(val):
                if val in ('', 'None', None):
                    return "-"
                try:
                    f = float(val)
                    return f"{f:+.2f}%"
                except:
                    return "-"
            
            display_rows.append({
                't 日': rec.get('target_date', ''),
                't-1 日': rec.get('signal_date', ''),
                '信号': f"{sig_icon} {sig_type}",
                'ΔNE': rec.get('d_ne_skew', '-'),
                'ΔIVR': rec.get('d_iv_rank', '-'),
                'ΔDR': rec.get('d_delta_ratio', '-'),
                '30min%': fmt_ret(rec.get('ret_30m', '')),
                '60min%': fmt_ret(rec.get('ret_60m', '')),
                '收盘%': fmt_ret(rec.get('ret_eod', '')),
                '30min': verify_icon(rec.get('verified_30m', '')),
                '60min': verify_icon(rec.get('verified_60m', '')),
                '收盘': verify_icon(rec.get('verified_eod', '')),
            })
        
        records_df = pd.DataFrame(display_rows)
        st.dataframe(records_df, hide_index=True, use_container_width=True)
        
        st.caption("✅ = 信号方向正确 | ❌ = 信号方向错误 | ⏳ = 尚未验证（数据未到时间或非交易日）")
        
        # ---------- 手动验证按钮 ----------
        col_manual1, col_manual2 = st.columns([1, 3])
        with col_manual1:
            if st.button("🔄 手动重新验证全部", use_container_width=True):
                st.cache_data.clear()
                
                with st.spinner("正在重新验证..."):
                    re_verified = 0
                    for rec in sorted_records:
                        target_date = rec.get('target_date', '')
                        sig_type = rec.get('signal', '')
                        if sig_type not in ('LONG', 'SHORT'):
                            continue
                        
                        result, err = verify_signal_for_date(target_date, sig_type)
                        if result:
                            update_verification_in_gsheets(target_date, result)
                            re_verified += 1
                    
                    st.success(f"✅ 已重新验证 {re_verified} 条记录")
                    st.rerun()
        with col_manual2:
            st.caption("💡 t 日早盘走完后系统会自动验证；如需立即重新拉取价格点这里")
    
    # ---------- 页脚 ----------
    st.markdown("---")
    st.caption(
        f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
        f"🎯 三因素早盘信号 v2.0 | "
        f"基于 33 样本回测 (2026-01-15 ~ 2026-03-06)"
    )


def get_setup_guide():
    """Google Sheets 配置指南"""
    return """
### 配置 Google Sheets 只需 5 步

**1. 创建 Google Cloud 项目并启用 API**
- 访问 https://console.cloud.google.com
- 创建新项目（或用已有项目）
- 在 "APIs & Services" → "Library" 中启用：
  - Google Sheets API
  - Google Drive API

**2. 创建 Service Account**
- "APIs & Services" → "Credentials" → Create credentials → Service account
- 起个名字（例如 `streamlit-sheets`）
- 完成后点击进入这个 service account
- "Keys" → Add key → Create new key → 选 **JSON** → 下载

**3. 创建 Google Sheets 文档**
- 新建一个 Google Sheets，命名为 **`ThreeFactor_Signals`**
- 把刚才下载的 JSON 文件里的 `client_email` (类似 `xxx@xxx.iam.gserviceaccount.com`) 添加为这个 sheet 的 Editor

**4. 在 Streamlit Cloud 配置 Secrets**
- 打开你的 Streamlit Cloud app → Settings → Secrets
- 把 JSON 文件内容粘贴成这个格式：

```toml
[gcp_service_account]
type = "service_account"
project_id = "你的项目ID"
private_key_id = "..."
private_key = "-----BEGIN PRIVATE KEY-----\\n...\\n-----END PRIVATE KEY-----\\n"
client_email = "xxx@xxx.iam.gserviceaccount.com"
client_id = "..."
auth_uri = "https://accounts.google.com/o/oauth2/auth"
token_uri = "https://oauth2.googleapis.com/token"
auth_provider_x509_cert_url = "..."
client_x509_cert_url = "..."
```

⚠️ `private_key` 字段要把 JSON 里的 `\\n` 保留为转义字符（不是真换行）

**5. 重启 app**
保存 secrets 后 app 会自动重启，看到 ☁️ Google Sheets 已连接 就配置好了
"""


if __name__ == "__main__":
    main()
