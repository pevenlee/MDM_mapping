import streamlit as st
import pandas as pd
import json
import time
import os
from google import genai
from google.genai import types
from rapidfuzz import process, fuzz 

# ================= 1. 配置与初始化 =================

st.set_page_config(page_title="LinkMed Matcher", layout="wide", page_icon="🔗")

# API Key 配置
try:
    FIXED_API_KEY = st.secrets["GENAI_API_KEY"]
except:
    FIXED_API_KEY = "" 

# ✅ 修改配置：直接指定根目录下的主数据文件名
LOCAL_MASTER_FILE = "mdm_retial.xlsx"

# ================= 2. 核心工具函数 =================

@st.cache_resource
def get_client():
    if not FIXED_API_KEY: return None
    return genai.Client(api_key=FIXED_API_KEY, http_options={'api_version': 'v1beta'})

def safe_generate(client, prompt, response_schema=None):
    """安全调用 Gemini API"""
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
        return json.loads(response.text)
    except Exception as e:
        return {"error": str(e)}

@st.cache_data
def load_master_data():
    """
    加载本地主数据
    """
    if os.path.exists(LOCAL_MASTER_FILE):
        try:
            # 读取 CSV
            df = pd.read_csv(LOCAL_MASTER_FILE)
            # 简单清洗：确保核心列存在且去重（防止主数据有重复）
            if 'esid' in df.columns:
                df = df.drop_duplicates(subset=['esid'])
            return df
        except Exception as e:
            st.error(f"读取主数据文件出错: {e}")
            return pd.DataFrame()
    else:
        st.error(f"⚠️ 在根目录下未找到文件: {LOCAL_MASTER_FILE}。请检查文件名是否完全一致。")
        return pd.DataFrame()

def smart_map_columns(client, df_user, master_cols):
    """
    利用 AI 自动识别用户上传文件的列。
    """
    sample_data = df_user.head(3).to_markdown(index=False)
    
    prompt = f"""
    你是一个数据映射专家。
    【主数据核心列】: {master_cols} (重点关注: 标准名称, 地址)
    【用户上传数据预览】:
    {sample_data}
    
    请分析用户的列名，找出代表“药房名称/终端名称”的那一列。
    如果能找到“地址”列，也请一并指出。
    
    返回 JSON: {{ "name_col": "用户药房名列", "addr_col": "用户地址列(如无则null)" }}
    """
    
    res = safe_generate(client, prompt)
    return res

def get_candidates(query, choices, limit=5):
    """
    使用 RapidFuzz 获取 Top N 候选集。
    choices: dict {index: 标准名称}
    """
    if not isinstance(query, str) or not query.strip():
        return []
        
    results = process.extract(query, choices, limit=limit, scorer=fuzz.WRatio)
    return [r[2] for r in results] # 返回主数据的 index

def ai_match_row(client, user_row, name_col, addr_col, candidates_df):
    """
    AI 裁判逻辑：结合名称和地址进行综合判断
    """
    # 提取用户数据
    user_name = str(user_row.get(name_col, ''))
    user_addr = str(user_row.get(addr_col, '')) if addr_col else "未知"
    
    # 构造更精简的候选集信息
    # 对应你的表结构：标准名称, 地址, 省, 市, 区
    cols_to_keep = ['esid', '标准名称', '别名', '省', '市', '区', '地址']
    valid_cols = [c for c in cols_to_keep if c in candidates_df.columns]
    
    candidates_json = candidates_df[valid_cols].to_json(orient="records", force_ascii=False)
    
    prompt = f"""
    【任务】
    请判断“待匹配数据”是否与“候选主数据”中的某一条是同一家药店。
    
    【待匹配数据】
    - 名称: "{user_name}"
    - 地址/线索: "{user_addr}"
    
    【候选主数据列表】(已按名称相似度粗筛):
    {candidates_json}
    
    【判断逻辑】
    1. **名称匹配**: 忽略有限公司后缀、分店名的括号差异。
    2. **地址校验**: 如果待匹配数据有地址，请优先匹配地址（省市区+详细地址）最接近的候选。
    3. **唯一性**: 必须返回唯一的最佳匹配。
    
    【输出格式 JSON】
    {{
        "match_esid": "对应的esid" (如果无匹配返回 null),
        "match_name": "对应的标准名称",
        "confidence": "High/Medium/Low",
        "reason": "简述理由"
    }}
    """
    return safe_generate(client, prompt)

# ================= 3. 页面 UI =================

st.markdown("""
    <style>
    .stApp {background-color: #F8F9FA;}
    .main-header {font-size: 26px; font-weight: bold; color: #1E3A8A; margin-bottom: 20px;}
    .step-card {background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 5px rgba(0,0,0,0.05); margin-bottom: 15px;}
    </style>
    <div class="main-header">🔗 LinkMed 主数据匹配工具 (Local)</div>
""", unsafe_allow_html=True)

client = get_client()

# 加载主数据
with st.spinner("正在加载本地主数据..."):
    df_master = load_master_data()

# --- Sidebar ---
with st.sidebar:
    st.header("🗄️ 主数据看板")
    if not df_master.empty:
        st.success(f"✅ 已加载 {len(df_master)} 条记录")
        st.caption(f"来源: {LOCAL_MASTER_FILE}")
        st.caption("字段预览:")
        st.code("\n".join(df_master.columns[:8]) + "...", language="text")
    else:
        st.error("❌ 主数据加载失败")

# --- Step 1: 上传 ---
st.markdown('<div class="step-card"><h3>📂 1. 上传待清洗文件</h3></div>', unsafe_allow_html=True)
uploaded_file = st.file_uploader("支持 Excel/CSV (需包含药房名称)", type=['xlsx', 'csv'])

if uploaded_file and not df_master.empty:
    try:
        if uploaded_file.name.endswith('.csv'):
            df_user = pd.read_csv(uploaded_file)
        else:
            df_user = pd.read_excel(uploaded_file)
        
        st.dataframe(df_user.head(3), hide_index=True, use_container_width=True)
        
        # --- Step 2: 映射 ---
        st.markdown('<div class="step-card"><h3>🤖 2. 智能字段映射</h3></div>', unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        
        # 自动探测
        if 'map_config' not in st.session_state:
            with st.spinner("AI 正在分析表头..."):
                st.session_state.map_config = smart_map_columns(client, df_user, df_master.columns.tolist())
        
        map_res = st.session_state.map_config
        all_cols = df_user.columns.tolist()
        
        with col1:
            default_name = map_res.get('name_col') if map_res.get('name_col') in all_cols else all_cols[0]
            target_name_col = st.selectbox("📍 药房名称列", all_cols, index=all_cols.index(default_name))
            
        with col2:
            default_addr = map_res.get('addr_col')
            default_idx = all_cols.index(default_addr) if default_addr in all_cols else None
            target_addr_col = st.selectbox("🏠 地址列 (可选，提高精度)", [None] + all_cols, index=default_idx if default_idx else 0)

        # --- Step 3: 匹配 ---
        st.markdown('<div class="step-card"><h3>🚀 3. 执行匹配</h3></div>', unsafe_allow_html=True)
        
        run_btn = st.button("开始 AI 匹配", type="primary", use_container_width=True)
        
        if run_btn:
            results = []
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # 构建查找字典： {index: 标准名称}
            master_choices = df_master['标准名称'].fillna('').astype(str).to_dict()
            total_rows = len(df_user)
            
            for idx, row in df_user.iterrows():
                # 获取用户输入的名称
                raw_name = str(row[target_name_col])
                
                # 1. 粗筛
                candidate_indices = get_candidates(raw_name, master_choices, limit=5)
                
                if not candidate_indices:
                    res_row = {"原始输入": raw_name, "匹配ESID": None, "匹配标准名": None, "置信度": "Low", "理由": "无相似候选"}
                else:
                    # 2. 提取候选行详细数据
                    candidates_df = df_master.loc[candidate_indices].copy()
                    
                    # 3. AI 精判
                    ai_res = ai_match_row(client, row, target_name_col, target_addr_col, candidates_df)
                    
                    res_row = {
                        "原始输入": raw_name,
                        "匹配ESID": ai_res.get("match_esid"),
                        "匹配标准名": ai_res.get("match_name"),
                        "置信度": ai_res.get("confidence", "Low"),
                        "理由": ai_res.get("reason")
                    }
                
                results.append(res_row)
                progress_bar.progress((idx + 1) / total_rows)
                status_text.text(f"处理中: {raw_name} ...")
            
            status_text.success("✅ 匹配完成！")
            
            # --- 结果展示 ---
            df_result = pd.DataFrame(results)
            df_final = pd.concat([df_user.reset_index(drop=True), df_result.drop(columns=["原始输入"])], axis=1)
            
            def highlight_conf(val):
                color = '#d4edda' if val == 'High' else '#fff3cd' if val == 'Medium' else '#f8d7da'
                return f'background-color: {color}'

            st.dataframe(
                df_result.style.applymap(highlight_conf, subset=['置信度']),
                use_container_width=True
            )
            
            csv = df_final.to_csv(index=False).encode('utf-8-sig')
            st.download_button("📥 下载完整结果", csv, "matched_result.csv", "text/csv")

    except Exception as e:
        st.error(f"运行时发生错误: {str(e)}")
        st.exception(e)

else:
    if df_master.empty:
        st.warning("请确认 '表头.xlsx - Sheet1.csv' 已上传至根目录。")