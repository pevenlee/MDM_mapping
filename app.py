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

st.set_page_config(page_title="LinkMed Matcher Ultimate", layout="wide", page_icon="🧬")

try:
    FIXED_API_KEY = st.secrets["GENAI_API_KEY"]
except:
    FIXED_API_KEY = "" 

LOCAL_MASTER_FILE = "MDM_retail.xlsx"

# 初始化 Session State
if 'uploader_key' not in st.session_state:
    st.session_state.uploader_key = str(time.time())
if 'final_result_df' not in st.session_state:
    st.session_state.final_result_df = None
if 'match_stats' not in st.session_state:
    st.session_state.match_stats = {}

# ================= 2. 核心工具函数 =================

def reset_app():
    st.session_state.final_result_df = None
    st.session_state.match_stats = {}
    st.session_state.uploader_key = str(time.time())
    st.rerun()

@st.cache_resource
def get_client():
    if not FIXED_API_KEY: return None
    return genai.Client(api_key=FIXED_API_KEY, http_options={'api_version': 'v1beta'})

def safe_generate(client, prompt, response_schema=None, retries=3):
    if client is None: return {"error": "API Key 未配置"}
    wait_time = 2 
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
                return json.loads(response.text)
            except json.JSONDecodeError:
                return {"error": "JSON解析失败", "raw": response.text}
        except Exception as e:
            if "429" in str(e) or "503" in str(e):
                if attempt < retries - 1:
                    time.sleep(wait_time * (2 ** attempt))
                    continue
            return {"error": str(e)}
    return {"error": "Max retries reached"}

@st.cache_resource(show_spinner=False)
def load_master_data():
    """加载并建立地理索引"""
    if os.path.exists(LOCAL_MASTER_FILE):
        try:
            gc.collect()
            if LOCAL_MASTER_FILE.endswith('.xlsx'):
                df = pd.read_excel(LOCAL_MASTER_FILE, engine='openpyxl')
            else:
                df = pd.read_csv(LOCAL_MASTER_FILE)
            
            if 'esid' in df.columns: df = df.drop_duplicates(subset=['esid'])
            for col in ['标准名称', '省', '市', '区', '机构类型']:
                if col not in df.columns: df[col] = '' 
            
            df['标准名称'] = df['标准名称'].astype(str).str.strip()
            df['机构类型'] = df['机构类型'].astype(str).str.strip()
            
            geo_index = {
                'province': df.groupby('省').groups,
                'city': df.groupby('市').groups,
                'district': df.groupby('区').groups
            }
            return df, geo_index
        except Exception as e:
            st.error(f"读取主数据错误: {e}")
            return pd.DataFrame(), {}
    else:
        return pd.DataFrame(), {}

def smart_map_columns(client, df_user):
    user_cols = df_user.columns.tolist()
    sample_data = df_user.head(3).to_markdown(index=False)
    
    prompt = f"""
    分析用户数据，找出以下字段对应的列名。
    用户列名: {user_cols}
    预览: {sample_data}
    
    任务：找出以下列（如果没有则返回null）：
    1. name_col: 药房/终端名称
    2. chain_col: 连锁/品牌名称
    3. prov_col: 省份
    4. city_col: 城市/地级市
    5. dist_col: 区/县
    6. addr_col: 详细地址
    
    输出 JSON: {{ "name_col": "...", "chain_col": "...", "prov_col": "...", "city_col": "...", "dist_col": "...", "addr_col": "..." }}
    """
    res = safe_generate(client, prompt)
    if isinstance(res, list): res = res[0] if res else {}
    return res

def get_candidates_scoped(query, df_master, geo_index, user_row, mapping):
    """分层漏斗筛选逻辑"""
    u_prov = str(user_row[mapping['prov']]) if mapping['prov'] and pd.notna(user_row[mapping['prov']]) else ''
    u_city = str(user_row[mapping['city']]) if mapping['city'] and pd.notna(user_row[mapping['city']]) else ''
    u_dist = str(user_row[mapping['dist']]) if mapping['dist'] and pd.notna(user_row[mapping['dist']]) else ''
    
    subset_indices = []
    scope_level = "Global"

    if u_dist and u_dist in geo_index['district']:
        subset_indices = geo_index['district'][u_dist]
        scope_level = f"District ({u_dist})"
    elif u_city and u_city in geo_index['city']:
        subset_indices = geo_index['city'][u_city]
        scope_level = f"City ({u_city})"
    elif u_prov and u_prov in geo_index['province']:
        subset_indices = geo_index['province'][u_prov]
        scope_level = f"Province ({u_prov})"
    
    if len(subset_indices) > 0:
        candidate_subset = df_master.loc[subset_indices]
        choices = candidate_subset['标准名称'].fillna('').astype(str).to_dict()
    else:
        choices = df_master['标准名称'].fillna('').astype(str).to_dict()
        scope_level = "Global (No Geo Match)"

    if not query.strip(): return [], scope_level
    results = process.extract(query, choices, limit=5, scorer=fuzz.WRatio)
    return [r[2] for r in results], scope_level

def ai_match_row_advanced(client, user_row, search_name, scope_level, candidates_df):
    cols_to_keep = ['esid', '标准名称', '机构类型', '省', '市', '区', '地址']
    valid_cols = [c for c in cols_to_keep if c in candidates_df.columns]
    candidates_json = candidates_df[valid_cols].to_json(orient="records", force_ascii=False)
    
    prompt = f"""
    【任务】判断“待匹配实体”与“候选列表”中的哪一条是同一家机构。
    
    【待匹配实体】
    - 组合搜索名称: "{search_name}"
    - 地理筛选范围: {scope_level}
    - 原始行数据: {user_row.to_json(force_ascii=False)}
    
    【候选主数据】
    {candidates_json}
    
    【判断逻辑】
    1. **地理一致性**: 候选必须在同一城市/区县。
    2. **名称包含**: 搜索名称可能包含连锁名，候选可能不包含，需逻辑对齐。
    3. **机构类型**: 返回结果必须包含该候选的“机构类型”。
    
    【输出 JSON】
    {{
        "match_esid": "ESID or null",
        "match_name": "标准名称",
        "match_type": "机构类型",
        "confidence": "High/Low",
        "reason": "简短理由"
    }}
    """
    return safe_generate(client, prompt)

# ================= 3. 页面 UI =================

st.markdown("""
    <style>
    .stApp {background-color: #F8F9FA;}
    .stat-card {background: #ffffff; padding: 15px; border-radius: 8px; border: 1px solid #e5e7eb; box-shadow: 0 1px 2px rgba(0,0,0,0.05);}
    .big-num {font-size: 24px; font-weight: bold; color: #1e40af;}
    .sub-text {font-size: 14px; color: #6b7280;}
    .success-box {background-color: #dcfce7; color: #166534; padding: 10px; border-radius: 5px; border: 1px solid #bbf7d0; margin-bottom: 10px;}
    .info-box {background-color: #e0f2fe; color: #075985; padding: 10px; border-radius: 5px; border: 1px solid #bae6fd; margin-bottom: 10px;}
    </style>
    <div style="font-size: 26px; font-weight: bold; color: #1E3A8A; margin-bottom: 20px;">
    🧬 LinkMed Matcher (Pre-Filter Engine)
    </div>
""", unsafe_allow_html=True)

client = get_client()

# 加载数据 & 索引
df_master, geo_index = pd.DataFrame(), {}
if os.path.exists(LOCAL_MASTER_FILE):
    with st.spinner(f"正在加载主数据并构建地理索引..."):
        df_master, geo_index = load_master_data()
else:
    st.warning(f"⚠️ 文件缺失: `{LOCAL_MASTER_FILE}`")

# --- Sidebar ---
with st.sidebar:
    st.header("🗄️ 控制台")
    if st.button("🗑️ 清空重置", type="secondary", use_container_width=True):
        reset_app()
    if not df_master.empty:
        st.success(f"主数据: {len(df_master)} 条")

# --- 主流程 ---
if st.session_state.final_result_df is None:
    st.markdown("### 📂 1. 上传数据")
    uploaded_file = st.file_uploader("Excel/CSV", type=['xlsx', 'csv'], key=st.session_state.uploader_key)

    if uploaded_file and not df_master.empty:
        if uploaded_file.name.endswith('.csv'): df_user = pd.read_csv(uploaded_file)
        else: df_user = pd.read_excel(uploaded_file)
        
        # --- 2. 字段映射 ---
        st.markdown("### 🤖 2. 字段映射")
        if 'map_config' not in st.session_state or st.session_state.get('last_file') != uploaded_file.name:
            with st.spinner("AI 正在分析表头..."):
                st.session_state.map_config = smart_map_columns(client, df_user)
                st.session_state.last_file = uploaded_file.name
        
        map_res = st.session_state.map_config
        cols = df_user.columns.tolist()
        
        c1, c2, c3 = st.columns(3)
        def get_idx(key): return cols.index(map_res.get(key)) if map_res.get(key) in cols else 0
        
        with c1:
            col_name = st.selectbox("📍 药房名称", cols, index=get_idx('name_col'))
            col_chain = st.selectbox("🔗 连锁名称 (可选)", [None]+cols, index=cols.index(map_res['chain_col'])+1 if map_res.get('chain_col') in cols else 0)
        with c2:
            col_prov = st.selectbox("🗺️ 省份 (可选)", [None]+cols, index=cols.index(map_res['prov_col'])+1 if map_res.get('prov_col') in cols else 0)
            col_city = st.selectbox("🏙️ 城市 (可选)", [None]+cols, index=cols.index(map_res['city_col'])+1 if map_res.get('city_col') in cols else 0)
        with c3:
            col_dist = st.selectbox("🏘️ 区县 (可选)", [None]+cols, index=cols.index(map_res['dist_col'])+1 if map_res.get('dist_col') in cols else 0)
            col_addr = st.selectbox("🏠 详细地址 (可选)", [None]+cols, index=cols.index(map_res['addr_col'])+1 if map_res.get('addr_col') in cols else 0)

        mapping = {
            'prov': col_prov, 'city': col_city, 'dist': col_dist, 
            'addr': col_addr, 'chain': col_chain, 'name': col_name
        }

        # --- 🌟 3. 预处理分流 (Pre-Filter) ---
        st.markdown("### ⚡ 3. 预处理与执行")
        
        # 实时计算全字匹配，不消耗 Token，速度极快
        master_exact = df_master.drop_duplicates(subset=['标准名称']).set_index('标准名称').to_dict('index')
        
        exact_rows_data = []
        remaining_indices = []
        
        # 遍历一遍用户数据，进行分流
        for idx, row in df_user.iterrows():
            raw_name = str(row[col_name]).strip()
            chain_name = str(row[col_chain]).strip() if col_chain and pd.notna(row[col_chain]) else ""
            
            search_name = raw_name
            if chain_name and chain_name not in raw_name:
                search_name = f"{chain_name} {raw_name}"
            
            if search_name in master_exact:
                m = master_exact[search_name]
                # 预填结果
                res = row.to_dict()
                res.update({
                    "匹配ESID": m.get('esid'),
                    "匹配标准名": search_name,
                    "机构类型": m.get('机构类型'),
                    "置信度": "High",
                    "匹配方式": "全字匹配",
                    "理由": "精确命中 (预处理)"
                })
                exact_rows_data.append(res)
            else:
                remaining_indices.append(idx)
        
        # 创建分流后的 DataFrame
        df_exact_pre = pd.DataFrame(exact_rows_data)
        df_remaining = df_user.loc[remaining_indices].copy()
        
        count_exact = len(df_exact_pre)
        count_rem = len(df_remaining)
        
        # --- 4. 可视化反馈 ---
        st.markdown(f"""
        <div class="success-box">✅ <b>已自动命中 {count_exact} 行</b> (无需模型，直接通过)</div>
        <div class="info-box">⏳ <b>剩余 {count_rem} 行</b> 待模型智能匹配</div>
        """, unsafe_allow_html=True)
        
        if count_rem > 0:
            btn_text = f"🚀 开始匹配剩余 {count_rem} 行"
            btn_type = "primary"
        else:
            btn_text = "✨ 直接生成结果 (全部命中)"
            btn_type = "secondary"

        if st.button(btn_text, type=btn_type):
            
            # 如果还有剩余数据，跑模型
            ai_results_data = []
            stats = {'total': len(df_user), 'exact': count_exact, 'high': 0, 'low': 0, 'no_match': 0}
            
            if count_rem > 0:
                prog = st.progress(0)
                status = st.empty()
                
                for i, (orig_idx, row) in enumerate(df_remaining.iterrows()):
                    try:
                        # 重新构建 search_name (虽然上面构建过，但在循环里需要给get_candidate用)
                        raw_name = str(row[col_name]).strip()
                        chain_name = str(row[col_chain]).strip() if col_chain and pd.notna(row[col_chain]) else ""
                        search_name = raw_name
                        if chain_name and chain_name not in raw_name:
                            search_name = f"{chain_name} {raw_name}"

                        # 地理分层检索
                        indices, scope = get_candidates_scoped(search_name, df_master, geo_index, row, mapping)
                        
                        base_res = row.to_dict()
                        
                        if not indices:
                            base_res.update({
                                "匹配ESID": None, "匹配标准名": None, "机构类型": None,
                                "置信度": "Low", "匹配方式": "无结果", "理由": "无相似候选"
                            })
                            stats['no_match'] += 1
                        else:
                            candidates = df_master.loc[indices].copy()
                            ai_res = ai_match_row_advanced(client, row, search_name, scope, candidates)
                            
                            if isinstance(ai_res, list): ai_res = ai_res[0] if ai_res else {}
                            
                            conf = ai_res.get("confidence", "Low")
                            base_res.update({
                                "匹配ESID": ai_res.get("match_esid"),
                                "匹配标准名": ai_res.get("match_name"),
                                "机构类型": ai_res.get("match_type"),
                                "置信度": conf,
                                "匹配方式": f"模型匹配 ({scope})",
                                "理由": ai_res.get("reason")
                            })
                            
                            if conf == "High": stats['high'] += 1
                            else: stats['low'] += 1
                            
                            time.sleep(1.5) # 冷却
                            
                        ai_results_data.append(base_res)
                        prog.progress((i+1)/count_rem)
                        status.text(f"Processing ({i+1}/{count_rem}): {search_name}")
                        
                    except Exception as e:
                        st.error(f"Error at index {orig_idx}: {e}")
                        break
            
            # --- 5. 合并结果 ---
            # 将 df_exact_pre 和 ai_results_data 合并
            if ai_results_data:
                df_ai_res = pd.DataFrame(ai_results_data)
                df_final = pd.concat([df_exact_pre, df_ai_res], ignore_index=True)
            else:
                df_final = df_exact_pre
            
            # (可选) 如果想尽量保持原始顺序，可以这里不做排序，或者如果需要的话
            # df_final = df_final.reindex(df_user.index) # 只有当我们在上面保留了原始索引时才有效
            # 简单起见，我们直接把全字匹配放前面，模型放后面，用户通常更喜欢这样
            
            st.session_state.final_result_df = df_final
            st.session_state.match_stats = stats
            st.rerun()

# --- 4. 结果与统计展示 ---
if st.session_state.final_result_df is not None:
    s = st.session_state.match_stats
    total = s.get('total', len(st.session_state.final_result_df))
    if total == 0: total = 1
    
    st.markdown("### 📊 匹配统计报告")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(f"""
        <div class="stat-card">
            <div class="sub-text">🎯 全字匹配</div>
            <div class="big-num">{s['exact']} 行</div>
            <div style="color:green; font-weight:bold;">{s['exact']/total:.1%}</div>
        </div>
        """, unsafe_allow_html=True)
        
    with col2:
        model_total = s['high'] + s['low'] + s['no_match']
        if model_total == 0: model_total = 1 # 防止分母为0
        
        real_model_count = s['high'] + s['low'] # 不包含直接no_match的，或者包含看定义
        
        st.markdown(f"""
        <div class="stat-card">
            <div class="sub-text">🤖 模型处理</div>
            <div class="big-num">{real_model_count} 行</div>
            <div style="color:blue; font-weight:bold;">{real_model_count/total:.1%}</div>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        st.markdown(f"""
        <div class="stat-card">
            <div class="sub-text">🔥 High 置信度</div>
            <div class="big-num">{s['high']} 行</div>
            <div class="sub-text">占模型: {s['high']/model_total:.1%}</div>
        </div>
        """, unsafe_allow_html=True)

    with col4:
        st.markdown(f"""
        <div class="stat-card">
            <div class="sub-text">⚠️ Low 置信度</div>
            <div class="big-num">{s['low']} 行</div>
            <div class="sub-text">占模型: {s['low']/model_total:.1%}</div>
        </div>
        """, unsafe_allow_html=True)

    st.divider()
    
    def color_row(row):
        if row['匹配方式'] == '全字匹配': return ['background-color: #dcfce7'] * len(row)
        if row.get('置信度') == 'High': return ['background-color: #e0f2fe'] * len(row)
        return [''] * len(row)

    df_show = st.session_state.final_result_df
    # 调整列顺序，把匹配结果放前面
    cols = list(df_show.columns)
    priority_cols = ['原始输入', '匹配ESID', '匹配标准名', '机构类型', '置信度', '理由']
    other_cols = [c for c in cols if c not in priority_cols]
    # 注意：原始输入可能在 df_exact_pre 里没有被统一命名，这里我们在构建字典时要注意
    # 代码中 df_exact_pre 已经包含了 '原始输入' 等key，可以直接 concat
    
    st.dataframe(df_show.style.apply(color_row, axis=1), use_container_width=True)
    
    csv = df_show.to_csv(index=False).encode('utf-8-sig')
    st.download_button("📥 下载完整报告", csv, "linkmed_final_result.csv", "text/csv", type="primary")
