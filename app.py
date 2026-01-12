import streamlit as st
import pandas as pd
import json
import time
import os
import gc
import random
from google import genai
from google.genai import types
from rapidfuzz import process, fuzz 

# ================= 1. 配置与初始化 =================

st.set_page_config(page_title="LinkMed Matcher Pro", layout="wide", page_icon="⚡")

try:
    FIXED_API_KEY = st.secrets["GENAI_API_KEY"]
except:
    FIXED_API_KEY = "" 

LOCAL_MASTER_FILE = "MDM_retail.xlsx"

# 初始化 Session State
if 'uploader_key' not in st.session_state:
    st.session_state.uploader_key = str(time.time())

# ================= 2. 核心工具函数 =================

def reset_app():
    """重置 App 状态"""
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    st.session_state.uploader_key = str(time.time())
    st.rerun()

@st.cache_resource
def get_client():
    if not FIXED_API_KEY: return None
    return genai.Client(api_key=FIXED_API_KEY, http_options={'api_version': 'v1beta'})

def safe_generate(client, prompt, response_schema=None, retries=3):
    """
    带重试机制的 AI 调用函数 (防止 429 错误)
    """
    if client is None:
        return {"error": "API Key 未配置"}
    
    wait_time = 2 # 初始等待秒数
    
    for attempt in range(retries):
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
            error_str = str(e)
            # 识别 API 频率限制错误 (429) 或 服务过载 (503)
            if "429" in error_str or "503" in error_str or "Resource exhausted" in error_str:
                if attempt < retries - 1:
                    sleep_time = wait_time * (2 ** attempt) # 指数退避: 2s, 4s, 8s
                    st.toast(f"⚠️ API 繁忙，正在冷却 {sleep_time} 秒后重试...", icon="⏳")
                    time.sleep(sleep_time)
                    continue
            
            return {"error": str(e)}
            
    return {"error": "达到最大重试次数，调用失败"}

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
    
    # 修改Prompt：增加对连锁/品牌列的识别
    prompt = f"""
    你是一个数据清洗专家。请分析用户上传数据的表头和前几行数据。
    【用户列名列表】: {user_cols}
    【用户数据预览】: {sample_data}
    
    【任务】：
    1. "name_col": 最代表“门店名称/药房名称”的列。
    2. "addr_col": 最代表“详细地址”的列（如果没有则为null）。
    3. "chain_col": 最代表“连锁品牌/总店名称”的列（例如：海王星辰、大参林），这有助于增强识别。如果没有则为null。
    
    【输出 JSON】: {{ "name_col": "...", "addr_col": "...", "chain_col": "..." }}
    """
    res = safe_generate(client, prompt)
    if isinstance(res, list): res = res[0] if res else {}
    return res

def get_candidates(query, choices, limit=5):
    if not isinstance(query, str) or not query.strip():
        return []
    # 使用 WRatio 处理部分匹配
    results = process.extract(query, choices, limit=limit, scorer=fuzz.WRatio)
    return [r[2] for r in results]

def ai_match_row_smart(client, user_row, name_col, addr_col, chain_col, candidates_df):
    """
    智能增强匹配逻辑
    """
    # 1. 构建智能上下文
    u_name = str(user_row.get(name_col, '')).strip()
    u_addr = str(user_row.get(addr_col, '')).strip()
    u_chain = str(user_row.get(chain_col, '')).strip() if chain_col else ""
    
    # 如果地址为空，标记为未知，并在 Prompt 中处理
    addr_context = u_addr if u_addr and u_addr.lower() != 'nan' else "【地址缺失】"
    
    # 组合名称 (如果有连锁名且名称里不包含连锁名，则拼上去)
    full_name_context = u_name
    if u_chain and u_chain not in u_name:
        full_name_context = f"{u_chain} {u_name}"
    
    # 2. 准备候选集
    cols_to_keep = ['esid', '标准名称', '别名', '省', '市', '区', '地址']
    valid_cols = [c for c in cols_to_keep if c in candidates_df.columns]
    candidates_json = candidates_df[valid_cols].to_json(orient="records", force_ascii=False)
    
    # 3. 高级 Prompt
    prompt = f"""
    【角色】你是一个资深的主数据匹配专家。你的任务是根据有限的信息，从候选列表中找出同一家实体。
    
    【待匹配输入信息】:
    - 核心名称: "{full_name_context}" (可能包含连锁名和分店名)
    - 原始名称: "{u_name}"
    - 提供的地址: "{addr_context}"
    - 连锁/品牌: "{u_chain}"
    
    【候选主数据列表】: 
    {candidates_json}
    
    【思维链规则】:
    1. **地址优先但灵活**: 如果输入地址存在，优先匹配地址最接近的（省市区+道路）。
    2. **地址缺失处理**: 如果输入显示【地址缺失】，则必须严格依赖“核心名称”和“省/市/区”字段进行逻辑推理。不要强行匹配不同城市的店。
    3. **名称组合逻辑**: 输入的“核心名称”结合了连锁品牌。请寻找候选列表中包含该品牌且分店名（如“南山店”、“一分店”）匹配的记录。
    4. **模糊容忍**: 允许“大药房”、“药店”、“有限公司”等后缀的差异。
    
    【输出 JSON】: 
    {{ 
      "match_esid": "匹配到的ESID (如果没有匹配则填 null)", 
      "match_name": "匹配到的标准名称", 
      "confidence": "High/Medium/Low", 
      "reason": "请简短说明理由，例如：'名称完全一致，地址高度吻合' 或 '地址缺失，但分店名独特且城市一致'" 
    }}
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
    <div class="main-header">⚡ LinkMed 极速匹配 (Anti-Ban & Smart Mode)</div>
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
    if st.button("🗑️ 清空任务 / 重新上传", type="secondary", use_container_width=True):
        reset_app()
    st.divider()
    st.caption("🛡️ 防封控机制已启动")
    st.caption("💾 支持断点自动保存")

# --- Step 1: 上传 ---
st.markdown('<div class="step-card"><h3>📂 1. 上传待清洗文件</h3></div>', unsafe_allow_html=True)
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
        st.markdown('<div class="step-card"><h3>🤖 2. 智能字段识别 (增强版)</h3></div>', unsafe_allow_html=True)
        
        if 'map_config' not in st.session_state or st.session_state.get('last_file') != uploaded_file.name:
            with st.spinner("AI 正在分析表头结构..."):
                st.session_state.map_config = smart_map_columns(client, df_user)
                st.session_state.last_file = uploaded_file.name
        
        map_res = st.session_state.map_config
        all_cols = df_user.columns.tolist()
        c1, c2, c3 = st.columns(3)
        
        with c1:
            s_name = map_res.get('name_col')
            idx_name = all_cols.index(s_name) if s_name in all_cols else 0
            target_name_col = st.selectbox(f"📍 药房名称 (AI建议: {s_name})", all_cols, index=idx_name)
            
        with c2:
            s_chain = map_res.get('chain_col')
            idx_chain = all_cols.index(s_chain) if s_chain in all_cols else 0
            # 默认为 None 除非 AI 很有把握
            default_chain_idx = idx_chain + 1 if s_chain in all_cols else 0
            target_chain_col = st.selectbox(f"🔗 连锁/品牌 (可选, AI建议: {s_chain})", [None] + all_cols, index=default_chain_idx)

        with c3:
            s_addr = map_res.get('addr_col')
            idx_addr = all_cols.index(s_addr) if s_addr in all_cols else 0
            default_addr_idx = idx_addr + 1 if s_addr in all_cols else 0
            target_addr_col = st.selectbox(f"🏠 地址 (可选, AI建议: {s_addr})", [None] + all_cols, index=default_addr_idx)

        # --- Step 3: 匹配 ---
        st.markdown('<div class="step-card"><h3>🚀 3. 执行匹配</h3></div>', unsafe_allow_html=True)
        
        run_btn = st.button(f"开始匹配 ({file_rows} 行)", type="primary", use_container_width=True)
        
        if run_btn:
            results = []
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # 准备全字匹配字典
            df_master_unique = df_master.drop_duplicates(subset=['标准名称'], keep='first')
            master_exact_lookup = df_master_unique.set_index('标准名称').to_dict('index')
            # 准备模糊匹配 Choices (这里我们用更丰富的文本做索引，如果有别名更好)
            master_choices = df_master['标准名称'].fillna('').astype(str).to_dict()
            
            exact_count = 0
            model_count = 0
            error_flag = False
            
            # --- 核心循环 (带断点保护) ---
            start_time = time.time()
            
            # 使用 try-except 包裹循环外部，捕获非预期的致命错误
            try:
                for idx, row in df_user.iterrows():
                    
                    # 允许用户通过停止 Streamlit 运行来中断，这里我们模拟检测（Streamlit原生不支持循环中断按钮，只能依赖外部停止）
                    # 实际上如果发生异常，我们会 break
                    
                    try:
                        raw_name = str(row[target_name_col]).strip()
                        
                        # --- 策略 A: 全字匹配 (最快，0成本) ---
                        if raw_name in master_exact_lookup:
                            match_data = master_exact_lookup[raw_name]
                            res_row = {
                                "原始输入": raw_name, "匹配ESID": match_data.get('esid'),
                                "匹配标准名": raw_name, "置信度": "High",
                                "理由": "完全匹配", "匹配方式": "全字匹配"
                            }
                            exact_count += 1
                            # 全字匹配不需要冷却，但为了UI流畅
                            time.sleep(0.001) 
                            
                        else:
                            # --- 策略 B: 模型匹配 (消耗 Token) ---
                            
                            # 1. 粗筛 (RapidFuzz)
                            # 如果有连锁名，拼接到搜索词里增加粗筛准确度
                            search_query = raw_name
                            if target_chain_col and row[target_chain_col]:
                                chain_val = str(row[target_chain_col])
                                if chain_val not in raw_name:
                                    search_query = f"{chain_val} {raw_name}"

                            candidate_indices = get_candidates(search_query, master_choices, limit=5)
                            
                            if not candidate_indices:
                                res_row = {
                                    "原始输入": raw_name, "匹配ESID": None, "匹配标准名": None, 
                                    "置信度": "Low", "理由": "无相似候选", "匹配方式": "无结果"
                                }
                            else:
                                candidates_df = df_master.loc[candidate_indices].copy()
                                
                                # 2. 调用 AI (使用新版 smart 函数)
                                ai_res = ai_match_row_smart(client, row, target_name_col, target_addr_col, target_chain_col, candidates_df)
                                
                                # 防御列表
                                if isinstance(ai_res, list): ai_res = ai_res[0] if ai_res else {}
                                if ai_res.get("error"):
                                    # 如果 AI 返回了错误信息（比如重试都失败了）
                                    res_row = {
                                        "原始输入": raw_name, "匹配ESID": None, "匹配标准名": None,
                                        "置信度": "Error", "理由": ai_res.get("error"), "匹配方式": "API错误"
                                    }
                                else:
                                    res_row = {
                                        "原始输入": raw_name,
                                        "匹配ESID": ai_res.get("match_esid"),
                                        "匹配标准名": ai_res.get("match_name"),
                                        "置信度": ai_res.get("confidence", "Low"),
                                        "理由": ai_res.get("reason"),
                                        "匹配方式": "模型匹配"
                                    }
                                
                                # 🛡️ 防封控：强制冷却
                                # 每次 AI 调用后等待 1.5 秒
                                time.sleep(1.5) 
                                
                            model_count += 1
                        
                        results.append(res_row)
                        
                        # 更新UI
                        progress_bar.progress((idx + 1) / file_rows)
                        status_text.text(f"[{idx+1}/{file_rows}] 处理中: {raw_name}")
                        
                    except Exception as inner_e:
                        # 捕获单行处理错误，不中断整个流程，或者选择中断保存
                        st.error(f"处理第 {idx+1} 行时发生错误: {inner_e}")
                        # 这里选择中断循环，保存已有的结果
                        error_flag = True
                        break
            
            except Exception as outer_e:
                st.error(f"严重错误中断: {outer_e}")
                error_flag = True

            # --- 结果处理 (无论是否发生错误都会执行) ---
            
            if error_flag:
                st.warning(f"⚠️ 匹配过程意外中断。已为您保存前 {len(results)} 条结果。")
            else:
                st.success(f"✅ 全部完成! 全字匹配: {exact_count} | 模型匹配: {model_count}")
            
            if results:
                df_result = pd.DataFrame(results)
                df_final = pd.concat([df_user.iloc[:len(results)].reset_index(drop=True), df_result.drop(columns=["原始输入"])], axis=1)
                
                def highlight_row(row):
                    if row['匹配方式'] == '全字匹配': return ['background-color: #d1fae5'] * len(row)
                    elif row['置信度'] == 'High': return ['background-color: #fff3cd'] * len(row)
                    elif row['置信度'] == 'Error': return ['background-color: #fca5a5'] * len(row)
                    else: return [''] * len(row)

                st.dataframe(df_result.style.apply(highlight_row, axis=1))
                
                csv = df_final.to_csv(index=False).encode('utf-8-sig')
                filename = "matched_result_partial.csv" if error_flag else "matched_result_final.csv"
                st.download_button(f"📥 下载结果 ({filename})", csv, filename, "text/csv")
            else:
                st.error("没有产生任何结果数据。")

    except Exception as e:
        st.error(f"初始化错误: {str(e)}")
        if st.button("🔄 重置环境"):
            reset_app()

else:
    if df_master.empty and os.path.exists(LOCAL_MASTER_FILE):
         st.info("正在初始化数据引擎...")
