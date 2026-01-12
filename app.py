import streamlit as st
import pandas as pd
import json
import time
import os
import gc
from google import genai
from google.genai import types
from rapidfuzz import process, fuzz 

# ================= 1. 配置与初始化 =================

st.set_page_config(page_title="LinkMed Matcher Pro", layout="wide", page_icon="⚡")

try:
    FIXED_API_KEY = st.secrets["GENAI_API_KEY"]
except:
    FIXED_API_KEY = "" 

# ✅ 指向 Excel 文件
LOCAL_MASTER_FILE = "MDM_retail.xlsx"

# 初始化 Session State 中的 uploader key，用于强制重置文件上传控件
if 'uploader_key' not in st.session_state:
    st.session_state.uploader_key = str(time.time())

# ================= 2. 核心工具函数 =================

def reset_app():
    """重置 App 状态，允许重新上传"""
    # 清除所有 session_state
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    # 重置 uploader key
    st.session_state.uploader_key = str(time.time())
    # 强制刷新页面
    st.rerun()

@st.cache_resource
def get_client():
    if not FIXED_API_KEY: return None
    return genai.Client(api_key=FIXED_API_KEY, http_options={'api_version': 'v1beta'})

def safe_generate(client, prompt, response_schema=None):
    if client is None:
        return {"error": "API Key 未配置"}
    try:
        config = types.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=response_schema
        )
        response = client.models.generate_content(
            model="gemini-2.0-flash", 
            contents=prompt,
            config=config
        )
        try:
            parsed = json.loads(response.text)
            return parsed
        except json.JSONDecodeError:
            return {"error": "JSON解析失败", "raw": response.text}
    except Exception as e:
        return {"error": str(e)}

@st.cache_resource(show_spinner=False)
def load_master_data():
    if os.path.exists(LOCAL_MASTER_FILE):
        try:
            gc.collect()
            if LOCAL_MASTER_FILE.endswith('.xlsx'):
                df = pd.read_excel(LOCAL_MASTER_FILE, engine='openpyxl')
            else:
                df = pd.read_csv(LOCAL_MASTER_FILE)
            
            if 'esid' in df.columns:
                df = df.drop_duplicates(subset=['esid'])
            if '标准名称' in df.columns:
                df['标准名称'] = df['标准名称'].astype(str).str.strip()
            return df
        except Exception as e:
            st.error(f"读取主数据文件出错: {e}")
            return pd.DataFrame()
    else:
        return pd.DataFrame()

def smart_map_columns(client, df_user):
    user_cols = df_user.columns.tolist()
    sample_data = df_user.head(3).to_markdown(index=False)
    prompt = f"""
    你是一个数据清洗专家。请分析用户上传数据的表头和前几行数据。
    【用户列名列表】: {user_cols}
    【用户数据预览】: {sample_data}
    【任务】：找出"药房名称"列(name_col)和"地址"列(addr_col)。
    【要求】：返回列名必须存在于列表中。
    【输出 JSON】: {{ "name_col": "...", "addr_col": "..." }}
    """
    res = safe_generate(client, prompt)
    if isinstance(res, list): res = res[0] if res else {}
    return res

def get_candidates(query, choices, limit=5):
    if not isinstance(query, str) or not query.strip():
        return []
    results = process.extract(query, choices, limit=limit, scorer=fuzz.WRatio)
    return [r[2] for r in results]

def ai_match_row(client, user_row, name_col, addr_col, candidates_df):
    user_name = str(user_row.get(name_col, ''))
    user_addr = str(user_row.get(addr_col, '')) if addr_col else "未知"
    cols_to_keep = ['esid', '标准名称', '别名', '省', '市', '区', '地址']
    valid_cols = [c for c in cols_to_keep if c in candidates_df.columns]
    candidates_json = candidates_df[valid_cols].to_json(orient="records", force_ascii=False)
    
    prompt = f"""
    【任务】判断“待匹配数据”是否与“候选主数据”是同一家药店。
    【待匹配】名称: "{user_name}", 地址: "{user_addr}"
    【候选集】: {candidates_json}
    【规则】优先匹配地址（省市区+详细地址）最接近的候选。
    【输出 JSON】: {{ "match_esid": "...", "match_name": "...", "confidence": "High/Medium/Low", "reason": "..." }}
    """
    return safe_generate(client, prompt)

# ================= 3. 页面 UI =================

st.markdown("""
    <style>
    .stApp {background-color: #F8F9FA;}
    .main-header {font-size: 26px; font-weight: bold; color: #1E3A8A; margin-bottom: 20px;}
    .step-card {background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 5px rgba(0,0,0,0.05); margin-bottom: 15px;}
    .count-box {
        background-color: #e3f2fd; color: #0d47a1; padding: 10px 15px; 
        border-radius: 5px; font-weight: bold; border-left: 5px solid #1976d2;
        margin: 10px 0; display: inline-block;
    }
    </style>
    <div class="main-header">⚡ LinkMed 极速匹配 (Pro)</div>
""", unsafe_allow_html=True)

client = get_client()

# 延迟加载主数据
df_master = pd.DataFrame()
if os.path.exists(LOCAL_MASTER_FILE):
    with st.spinner(f"正在加载主数据资源: {LOCAL_MASTER_FILE}..."):
        df_master = load_master_data()
else:
    st.warning(f"⚠️ 未检测到主数据文件: `{LOCAL_MASTER_FILE}`")

# --- Sidebar ---
with st.sidebar:
    st.header("🗄️ 控制台")
    
    # 🔥 全局重置按钮 🔥
    if st.button("🗑️ 清空任务 / 重新上传", type="secondary", use_container_width=True):
        reset_app()
        
    st.divider()
    st.subheader("主数据状态")
    if not df_master.empty:
        st.success(f"✅ 已加载 {len(df_master)} 条记录")
    else:
        st.info("等待加载...")

# --- Step 1: 上传 ---
st.markdown('<div class="step-card"><h3>📂 1. 上传待清洗文件</h3></div>', unsafe_allow_html=True)

# 使用动态 key，reset_app() 改变 key 后会强制重置这个组件
uploaded_file = st.file_uploader(
    "支持 Excel/CSV", 
    type=['xlsx', 'csv'], 
    key=st.session_state.get('uploader_key', 'default_key')
)

if uploaded_file and not df_master.empty:
    try:
        if uploaded_file.name.endswith('.csv'):
            df_user = pd.read_csv(uploaded_file)
        else:
            df_user = pd.read_excel(uploaded_file)
        
        file_rows = len(df_user)
        st.markdown(f'<div class="count-box">📊 读取成功: 共 {file_rows} 行数据</div>', unsafe_allow_html=True)
        st.dataframe(df_user.head(3), hide_index=True)
        
        # --- Step 2: 自动映射 ---
        st.markdown('<div class="step-card"><h3>🤖 2. 智能字段识别</h3></div>', unsafe_allow_html=True)
        
        if 'map_config' not in st.session_state or st.session_state.get('last_file') != uploaded_file.name:
            with st.spinner("AI 正在自动识别表头..."):
                st.session_state.map_config = smart_map_columns(client, df_user)
                st.session_state.last_file = uploaded_file.name
        
        map_res = st.session_state.map_config
        all_cols = df_user.columns.tolist()
        col1, col2 = st.columns(2)
        
        with col1:
            s_name = map_res.get('name_col')
            idx_name = all_cols.index(s_name) if s_name in all_cols else 0
            target_name_col = st.selectbox(f"📍 药房名称列 (AI建议: {s_name})", all_cols, index=idx_name)
            
        with col2:
            s_addr = map_res.get('addr_col')
            idx_addr = all_cols.index(s_addr) if s_addr in all_cols else 0
            target_addr_col = st.selectbox(f"🏠 地址列 (AI建议: {s_addr})", [None] + all_cols, index=idx_addr + 1 if s_addr in all_cols else 0)

        # --- Step 3: 匹配 ---
        st.markdown('<div class="step-card"><h3>🚀 3. 执行匹配</h3></div>', unsafe_allow_html=True)
        
        run_btn = st.button(f"开始匹配 ({file_rows} 行)", type="primary", use_container_width=True)
        
        if run_btn:
            results = []
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # 准备匹配数据
            df_master_unique = df_master.drop_duplicates(subset=['标准名称'], keep='first')
            master_exact_lookup = df_master_unique.set_index('标准名称').to_dict('index')
            master_choices = df_master['标准名称'].fillna('').astype(str).to_dict()
            
            exact_count = 0
            model_count = 0
            
            for idx, row in df_user.iterrows():
                raw_name = str(row[target_name_col]).strip()
                
                # --- 核心匹配逻辑 ---
                if raw_name in master_exact_lookup:
                    match_data = master_exact_lookup[raw_name]
                    res_row = {
                        "原始输入": raw_name, "匹配ESID": match_data.get('esid'),
                        "匹配标准名": raw_name, "置信度": "High",
                        "理由": "完全匹配", "匹配方式": "全字匹配"
                    }
                    exact_count += 1
                    time.sleep(0.005) 
                else:
                    candidate_indices = get_candidates(raw_name, master_choices, limit=5)
                    if not candidate_indices:
                        res_row = {
                            "原始输入": raw_name, "匹配ESID": None, "匹配标准名": None, 
                            "置信度": "Low", "理由": "无相似候选", "匹配方式": "无结果"
                        }
                    else:
                        candidates_df = df_master.loc[candidate_indices].copy()
                        ai_res = ai_match_row(client, row, target_name_col, target_addr_col, candidates_df)
                        if isinstance(ai_res, list): ai_res = ai_res[0] if ai_res else {}
                        
                        res_row = {
                            "原始输入": raw_name,
                            "匹配ESID": ai_res.get("match_esid"),
                            "匹配标准名": ai_res.get("match_name"),
                            "置信度": ai_res.get("confidence", "Low"),
                            "理由": ai_res.get("reason"),
                            "匹配方式": "模型匹配"
                        }
                    model_count += 1
                
                results.append(res_row)
                progress_bar.progress((idx + 1) / file_rows)
                status_text.text(f"[{idx+1}/{file_rows}] 处理中... {raw_name}")
            
            status_text.success(f"✅ 完成! 全字匹配: {exact_count} | 模型匹配: {model_count}")
            
            df_result = pd.DataFrame(results)
            df_final = pd.concat([df_user.reset_index(drop=True), df_result.drop(columns=["原始输入"])], axis=1)
            
            def highlight_row(row):
                if row['匹配方式'] == '全字匹配': return ['background-color: #d1fae5'] * len(row)
                elif row['置信度'] == 'High': return ['background-color: #fff3cd'] * len(row)
                else: return [''] * len(row)

            st.dataframe(df_result.style.apply(highlight_row, axis=1))
            csv = df_final.to_csv(index=False).encode('utf-8-sig')
            st.download_button("📥 下载结果", csv, "matched_result_pro.csv", "text/csv")

    except Exception as e:
        # 🔥🔥🔥 异常处理增强：提供重置按钮 🔥🔥🔥
        st.error(f"❌ 运行时发生异常: {str(e)}")
        st.exception(e)
        
        st.markdown("---")
        st.warning("检测到程序中断。您可以点击下方按钮重置环境并重新上传文件。")
        if st.button("🔄 重置并重新上传", type="primary"):
            reset_app()

else:
    if df_master.empty and os.path.exists(LOCAL_MASTER_FILE):
         st.info("正在初始化数据引擎...")