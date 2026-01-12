import streamlit as st
import pandas as pd
import json
import time
import os
from google import genai
from google.genai import types
from rapidfuzz import process, fuzz  # 需要安装: pip install rapidfuzz

# ================= 1. 配置与初始化 =================

st.set_page_config(page_title="LinkMed Matcher", layout="wide", page_icon="🔗")

# API Key 配置
try:
    FIXED_API_KEY = st.secrets["GENAI_API_KEY"]
except:
    FIXED_API_KEY = ""  # 建议在 secrets.toml 中配置

# 模拟 GitHub 主数据 URL (实际使用时替换为你的 raw.githubusercontent 链接)
# 这里为了演示，我将在代码中生成一个示例主数据 DataFrame
GITHUB_MASTER_DATA_URL = "https://raw.githubusercontent.com/your-repo/main/master_pharmacy.csv"

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
            model="gemini-2.0-flash", # 使用 Flash 模型速度更快，足以处理匹配任务
            contents=prompt,
            config=config
        )
        return json.loads(response.text)
    except Exception as e:
        return {"error": str(e)}

@st.cache_data
def load_master_data():
    """
    加载主数据。
    逻辑：优先从 GitHub 读取，如果失败(或未配置)则生成模拟数据。
    """
    try:
        # 实际代码：从 GitHub 读取 CSV
        # df = pd.read_csv(GITHUB_MASTER_DATA_URL)
        # return df
        
        # --- 演示用：模拟主数据 ---
        data = {
            "esid": ["MD001", "MD002", "MD003", "MD004", "MD005", "MD006"],
            "std_name": [
                "海王星辰健康药房(南山旗舰店)", 
                "大参林药房(广州天河路店)", 
                "国大药房(上海南京东路店)", 
                "老百姓大药房(长沙湘雅店)",
                "益丰大药房(常德步行街店)",
                "叮当快药(北京朝阳总仓)"
            ],
            "province": ["广东", "广东", "上海", "湖南", "湖南", "北京"],
            "address": ["深圳市南山区南海大道111号", "广州市天河区天河路200号", "上海市黄浦区南京东路", "长沙市开福区湘雅路", "常德市武陵区", "北京市朝阳区"]
        }
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"无法加载主数据: {e}")
        return pd.DataFrame()

def smart_map_columns(client, df_user, df_master):
    """
    利用 AI 自动识别用户上传文件的列。
    找出哪一列最可能是'药房名称'。
    """
    sample_data = df_user.head(3).to_markdown(index=False)
    master_cols = df_master.columns.tolist()
    
    prompt = f"""
    你是一个数据映射专家。
    这是主数据的列名: {master_cols} (核心列是药房名称)。
    这是用户上传的数据预览:
    {sample_data}
    
    请分析用户的列名和数据内容，找出代表“药房/客户名称”的那一列。
    返回 JSON: {{ "target_col": "用户表中的列名" }}
    """
    
    res = safe_generate(client, prompt)
    return res.get("target_col")

def get_candidates(query, choices, limit=5):
    """
    使用 RapidFuzz 获取 Top N 候选集。
    choices: dict {index: name_string}
    """
    # process.extract 返回 [(match_string, score, match_key), ...]
    results = process.extract(query, choices, limit=limit, scorer=fuzz.WRatio)
    return [r[2] for r in results] # 返回主数据的 index

def ai_match_row(client, raw_name, candidates_df):
    """
    AI 裁判逻辑：判断原始名称与候选集中哪一个匹配。
    """
    candidates_json = candidates_df.to_json(orient="records", force_ascii=False)
    
    prompt = f"""
    【任务】
    请将待匹配的原始名称，与候选主数据列表进行匹配。
    
    【待匹配原始名称】: "{raw_name}"
    
    【候选主数据列表】:
    {candidates_json}
    
    【规则】
    1. 忽略错别字、不规则的分隔符。
    2. "esid" 是唯一标识。
    3. 如果找到确信的匹配，confidence 返回 "High" 或 "Medium"。
    4. 如果所有候选看起来都不对，返回 null 并且 confidence 为 "Low"。
    
    【输出格式 JSON】
    {{
        "match_esid": "MDxxx" or null,
        "match_name": "标准名称" or null,
        "confidence": "High/Medium/Low",
        "reason": "简短理由"
    }}
    """
    return safe_generate(client, prompt)

# ================= 3. 页面 UI =================

st.markdown("""
    <style>
    .stApp {background-color: #F8F9FA;}
    .main-header {font-size: 28px; font-weight: bold; color: #1E3A8A; margin-bottom: 20px;}
    .step-card {background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 5px rgba(0,0,0,0.05); margin-bottom: 15px;}
    .success-tag {color: #059669; font-weight: bold;}
    </style>
    <div class="main-header">🔗 LinkMed Master Matcher</div>
""", unsafe_allow_html=True)

client = get_client()
df_master = load_master_data()

# --- Sidebar: 主数据状态 ---
with st.sidebar:
    st.header("🗄️ 主数据状态")
    if not df_master.empty:
        st.success(f"已加载主数据: {len(df_master)} 条")
        st.dataframe(df_master.head(), hide_index=True, height=200)
    else:
        st.error("主数据加载失败")
    
    st.info("💡 匹配逻辑：\n1. RapidFuzz 粗筛 (Top 5)\n2. Gemini AI 精判")

# --- Step 1: 上传文件 ---
st.markdown('<div class="step-card"><h3>📂 第一步：上传待匹配文件</h3></div>', unsafe_allow_html=True)
uploaded_file = st.file_uploader("支持 Excel/CSV", type=['xlsx', 'csv'])

if uploaded_file and not df_master.empty:
    try:
        if uploaded_file.name.endswith('.csv'):
            df_user = pd.read_csv(uploaded_file)
        else:
            df_user = pd.read_excel(uploaded_file)
        
        st.dataframe(df_user.head(3), hide_index=True)
        
        # --- Step 2: 智能列映射 ---
        st.markdown('<div class="step-card"><h3>🤖 第二步：列识别与配置</h3></div>', unsafe_allow_html=True)
        
        col1, col2 = st.columns([1, 1])
        with col1:
            # 自动探测
            with st.spinner("正在分析表头..."):
                detected_col = smart_map_columns(client, df_user, df_master)
            
            # 允许用户修正
            all_cols = df_user.columns.tolist()
            default_idx = all_cols.index(detected_col) if detected_col in all_cols else 0
            
            target_col = st.selectbox(
                "请确认包含【药房名称】的列:", 
                options=all_cols, 
                index=default_idx,
                help="AI 已自动推荐，如有误请手动修改"
            )

        with col2:
            st.info(f"将在主数据中匹配：**std_name** (及辅助字段 address)")

        # --- Step 3: 执行匹配 ---
        st.markdown('<div class="step-card"><h3>🚀 第三步：开始匹配</h3></div>', unsafe_allow_html=True)
        
        run_btn = st.button("开始 AI 匹配", type="primary", use_container_width=True)
        
        if run_btn:
            results = []
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # 准备主数据的查找字典 {index: std_name}
            # 实际上我们会把 esid 拼进去以增加区分度，或者只用 name
            master_lookup = df_master['std_name'].to_dict()
            
            total_rows = len(df_user)
            
            for idx, row in df_user.iterrows():
                raw_name = str(row[target_col])
                
                # 1. 粗筛：获取 Top 5 候选的 index
                candidate_indices = get_candidates(raw_name, master_lookup, limit=5)
                
                # 2. 从主数据中提取这 5 行完整信息 (含 esid, address 等)
                candidates_df = df_master.loc[candidate_indices].copy()
                
                # 3. AI 决策
                ai_res = ai_match_row(client, raw_name, candidates_df)
                
                # 4. 结果合并
                res_row = {
                    "原始输入": raw_name,
                    "匹配ESID": ai_res.get("match_esid"),
                    "匹配标准名": ai_res.get("match_name"),
                    "置信度": ai_res.get("confidence"),
                    "匹配理由": ai_res.get("reason")
                }
                results.append(res_row)
                
                # 更新进度
                progress_bar.progress((idx + 1) / total_rows)
                status_text.text(f"正在处理: {raw_name} ({idx+1}/{total_rows})")
            
            status_text.text("✅ 匹配完成！")
            
            # --- 结果展示与下载 ---
            df_result = pd.DataFrame(results)
            
            # 颜色高亮置信度
            def color_confidence(val):
                color = 'red' if val == 'Low' else 'orange' if val == 'Medium' else 'green'
                return f'color: {color}; font-weight: bold'
            
            st.dataframe(
                df_result.style.applymap(color_confidence, subset=['置信度']), 
                use_container_width=True
            )
            
            # 合并回原表供下载
            df_final = pd.concat([df_user.reset_index(drop=True), df_result.drop(columns=["原始输入"])], axis=1)
            
            csv = df_final.to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                label="📥 下载匹配结果 (CSV)",
                data=csv,
                file_name="matched_result.csv",
                mime="text/csv"
            )

    except Exception as e:
        st.error(f"处理文件时出错: {str(e)}")

else:
    if df_master.empty:
        st.warning("请先配置有效的 API Key 或检查主数据源。")
    else:
        st.info("👋 请上传文件开始工作。")