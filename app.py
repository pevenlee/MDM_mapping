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
    """加载并建立多维索引 (地理 + 连锁)"""
    if os.path.exists(LOCAL_MASTER_FILE):
        try:
            gc.collect()
            if LOCAL_MASTER_FILE.endswith('.xlsx'):
                df = pd.read_excel(LOCAL_MASTER_FILE, engine='openpyxl')
            else:
                df = pd.read_csv(LOCAL_MASTER_FILE)
            
            # 清洗
            if 'esid' in df.columns: df = df.drop_duplicates(subset=['esid'])
            cols_needed = ['标准名称', '省', '市', '区', '机构类型', '地址', '连锁品牌'] 
            for col in cols_needed:
                if col not in df.columns: df[col] = '' 
            
            for c in cols_needed:
                df[c] = df[c].astype(str).str.strip()
            
            # 1. 地理索引
            geo_index = {
                'province': df.groupby('省').groups,
                'city': df.groupby('市').groups,
                'district': df.groupby('区').groups
            }
            
            # 2. 连锁索引 (Chain Index) - 用于“总部匹配到所有门店”逻辑
            # 假设主数据有一列叫 '连锁品牌' 或类似，如果没有，可以尝试从标准名称提取（这里简化为必须有一列，或者用户指定列）
            # 为了通用性，我们暂时建立一个基于 '标准名称' 前缀的简单倒排索引是很难的。
            # 这里我们依赖用户上传时指定的 '连锁品牌' 列，或者主数据里有的 '连锁品牌' 列。
            # 如果主数据没有 '连锁品牌' 列，建议在 Excel 里先清洗出来。
            
            chain_groups = {}
            if '连锁品牌' in df.columns:
                # 过滤掉空的
                valid_chains = df[df['连锁品牌'].str.len() > 1]
                chain_groups = valid_chains.groupby('连锁品牌').groups
            
            return df, geo_index, chain_groups
        except Exception as e:
            st.error(f"读取主数据错误: {e}")
            return pd.DataFrame(), {}, {}
    else:
        return pd.DataFrame(), {}, {}

def smart_map_columns(client, df_user):
    user_cols = df_user.columns.tolist()
    sample_data = df_user.head(3).to_markdown(index=False)
    
    prompt = f"""
    分析用户数据，找出以下字段对应的列名。
    用户列名: {user_cols}
    预览: {sample_data}
    
    任务：找出以下列（如果没有则返回null）：
    1. name_col: 药房/终端名称
    2. chain_col: 连锁/品牌名称 (如: 海王星辰、大参林)
    3. prov_col: 省份
    4. city_col: 城市
    5. dist_col: 区/县
    6. addr_col: 详细地址
    
    输出 JSON: {{ "name_col": "...", "chain_col": "...", "prov_col": "...", "city_col": "...", "dist_col": "...", "addr_col": "..." }}
    """
    res = safe_generate(client, prompt)
    if isinstance(res, list): res = res[0] if res else {}
    return res

def get_candidates_hybrid(search_name, chain_name, df_master, geo_index, chain_groups, user_row, mapping):
    """
    🌟 混合检索逻辑：地理漏斗 + 连锁下钻
    """
    # 1. 确定地理范围索引
    u_prov = str(user_row[mapping['prov']]) if mapping['prov'] and pd.notna(user_row[mapping['prov']]) else ''
    u_city = str(user_row[mapping['city']]) if mapping['city'] and pd.notna(user_row[mapping['city']]) else ''
    u_dist = str(user_row[mapping['dist']]) if mapping['dist'] and pd.notna(user_row[mapping['dist']]) else ''
    
    geo_indices = set()
    scope_level = "Global"

    if u_dist and u_dist in geo_index['district']:
        geo_indices = set(geo_index['district'][u_dist])
        scope_level = f"District ({u_dist})"
    elif u_city and u_city in geo_index['city']:
        geo_indices = set(geo_index['city'][u_city])
        scope_level = f"City ({u_city})"
    elif u_prov and u_prov in geo_index['province']:
        geo_indices = set(geo_index['province'][u_prov])
        scope_level = f"Province ({u_prov})"
    else:
        # 全局模式，稍微危险，但如果没有地理信息只能这样
        geo_indices = set(df_master.index)
        scope_level = "Global (No Geo)"

    candidates_indices = set()

    # 2. 策略 A: 连锁下钻 (Chain Drill-Down) - 对应需求 1
    # 如果用户提供了连锁名，且在主数据中有该连锁的索引
    # 我们强制把该地理范围内的 *该连锁所有门店* 都加进来
    
    # 尝试从用户列获取连锁名，或者从名字中提取（简单包含判断）
    # 这里使用用户提供的 chain_name 参数
    if chain_name and chain_name in chain_groups:
        chain_store_indices = set(chain_groups[chain_name])
        # 取交集：该连锁 && 该地理范围
        valid_chain_stores = chain_store_indices.intersection(geo_indices)
        candidates_indices.update(valid_chain_stores)
        if len(valid_chain_stores) > 0:
            scope_level += " + Chain Drill-down"

    # 3. 策略 B: 模糊搜索 (Fuzzy Search)
    # 在地理范围内进行模糊搜索
    # 为了速度，如果 geo_indices 太大（>2000），我们可能只搜一部分，或者 RapidFuzz 足够快
    
    if geo_indices:
        # 提取当前范围内的名字字典
        current_scope_df = df_master.loc[list(geo_indices)]
        choices = current_scope_df['标准名称'].fillna('').astype(str).to_dict()
        
        # 模糊搜索前 5-8 名
        results = process.extract(search_name, choices, limit=8, scorer=fuzz.WRatio)
        for r in results:
            candidates_indices.add(r[2]) # r[2] is index

    return list(candidates_indices), scope_level

def ai_match_row_expert(client, user_row, search_name, chain_name, scope_level, candidates_df):
    
    # 准备 Prompt 数据
    cols_to_keep = ['esid', '标准名称', '机构类型', '省', '市', '区', '地址', '连锁品牌']
    valid_cols = [c for c in cols_to_keep if c in candidates_df.columns]
    candidates_json = candidates_df[valid_cols].to_json(orient="records", force_ascii=False)
    
    # 🌟🌟🌟 核心 Prompt 优化 🌟🌟🌟
    prompt = f"""
    【角色】你是一个精通地理位置的主数据匹配专家。
    
    【待匹配输入】
    - 搜索名称: "{search_name}"
    - 识别到的连锁品牌: "{chain_name}"
    - 地理范围: {scope_level}
    - 原始完整行: {user_row.to_json(force_ascii=False)}
    
    【候选主数据列表】 (已限制在相同地理范围内):
    {candidates_json}
    
    【匹配决策思维链】:
    1. **连锁总部陷阱**: 
       - 如果候选列表中包含“总部”、“总公司”、“股份有限公司”等非门店类型的记录，**除非输入明确指明是总部，否则不要匹配它们**。
       - 用户的真实意图通常是寻找该连锁在当地的**具体门店**。
       - 如果无法确定具体门店，宁可返回 Low 置信度，也不要错误匹配到总部。
    
    2. **地名交叉验证 (Cross-Field Check)**:
       - 用户的“搜索名称”中可能包含了地名或路名（例如输入：“海王星辰南山店” 或 “海王星辰人民路”）。
       - 请务必检查候选数据的**【地址】**列！
       - 如果候选的【标准名称】不匹配，但其【地址】包含了输入名称中的路名/地名，这是一个极强的匹配信号 (High Confidence)。
    
    3. **名称构建**:
       - 如果输入是 "连锁名 + 地名" (如 "大参林 东门")，请寻找名称或地址中包含 "东门" 的该连锁门店。
    
    【输出 JSON 格式】
    {{
        "match_esid": "匹配到的ESID (无匹配填null)",
        "match_name": "匹配到的标准名称",
        "match_type": "机构类型",
        "confidence": "High/Low",
        "reason": "请明确说明：是否通过地址交叉验证命中了？是否避开了总部？"
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
    🧬 LinkMed Matcher (Expert Logic)
    </div>
""", unsafe_allow_html=True)

client = get_client()

# 加载数据 & 索引
df_master, geo_index, chain_groups = pd.DataFrame(), {}, {}
if os.path.exists(LOCAL_MASTER_FILE):
    with st.spinner(f"正在加载主数据并构建多维索引..."):
        df_master, geo_index, chain_groups = load_master_data()
else:
    st.warning(f"⚠️ 文件缺失: `{LOCAL_MASTER_FILE}`")

# --- Sidebar ---
with st.sidebar:
    st.header("🗄️ 控制台")
    if st.button("🗑️ 清空重置", type="secondary", use_container_width=True):
        reset_app()
    if not df_master.empty:
        st.success(f"主数据: {len(df_master)} 条")
        st.caption(f"已识别连锁品牌数: {len(chain_groups)}")

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

        # --- 3. 预处理分流 ---
        st.markdown("### ⚡ 3. 预处理与执行")
        
        master_exact = df_master.drop_duplicates(subset=['标准名称']).set_index('标准名称').to_dict('index')
        exact_rows_data = []
        remaining_indices = []
        
        for idx, row in df_user.iterrows():
            raw_name = str(row[col_name]).strip()
            chain_name = str(row[col_chain]).strip() if col_chain and pd.notna(row[col_chain]) else ""
            
            # 构建用于全字匹配的名称
            search_name = raw_name
            if chain_name and chain_name not in raw_name:
                search_name = f"{chain_name} {raw_name}"
            
            if search_name in master_exact:
                m = master_exact[search_name]
                res = row.to_dict()
                res.update({
                    "匹配ESID": m.get('esid'),
                    "匹配标准名": search_name,
                    "机构类型": m.get('机构类型'),
                    "置信度": "High",
                    "匹配方式": "全字匹配",
                    "理由": "精确命中"
                })
                exact_rows_data.append(res)
            else:
                remaining_indices.append(idx)
        
        df_exact_pre = pd.DataFrame(exact_rows_data)
        df_remaining = df_user.loc[remaining_indices].copy()
        
        count_exact = len(df_exact_pre)
        count_rem = len(df_remaining)
        
        st.markdown(f"""
        <div class="success-box">✅ <b>已自动命中 {count_exact} 行</b></div>
        <div class="info-box">⏳ <b>剩余 {count_rem} 行</b> 待模型处理（已启用总部规避算法）</div>
        """, unsafe_allow_html=True)
        
        if count_rem > 0:
            btn_text = f"🚀 开始深度匹配剩余 {count_rem} 行"
            btn_type = "primary"
        else:
            btn_text = "✨ 直接生成结果"
            btn_type = "secondary"

        if st.button(btn_text, type=btn_type):
            
            ai_results_data = []
            stats = {'total': len(df_user), 'exact': count_exact, 'high': 0, 'low': 0, 'no_match': 0}
            
            if count_rem > 0:
                prog = st.progress(0)
                status = st.empty()
                
                for i, (orig_idx, row) in enumerate(df_remaining.iterrows()):
                    try:
                        raw_name = str(row[col_name]).strip()
                        chain_name = str(row[col_chain]).strip() if col_chain and pd.notna(row[col_chain]) else ""
                        
                        search_name = raw_name
                        if chain_name and chain_name not in raw_name:
                            search_name = f"{chain_name} {raw_name}"

                        # 🌟 调用混合检索 (Hybrid Retrieval)
                        indices, scope = get_candidates_hybrid(search_name, chain_name, df_master, geo_index, chain_groups, row, mapping)
                        
                        base_res = row.to_dict()
                        
                        if not indices:
                            base_res.update({
                                "匹配ESID": None, "匹配标准名": None, "机构类型": None,
                                "置信度": "Low", "匹配方式": "无结果", "理由": "无相似候选"
                            })
                            stats['no_match'] += 1
                        else:
                            candidates = df_master.loc[indices].copy()
                            # 🌟 调用专家级 Prompt
                            ai_res = ai_match_row_expert(client, row, search_name, chain_name, scope, candidates)
                            
                            if isinstance(ai_res, list): ai_res = ai_res[0] if ai_res else {}
                            
                            conf = ai_res.get("confidence", "Low")
                            base_res.update({
                                "匹配ESID": ai_res.get("match_esid"),
                                "匹配标准名": ai_res.get("match_name"),
                                "机构类型": ai_res.get("match_type"),
                                "置信度": conf,
                                "匹配方式": f"模型匹配",
                                "理由": ai_res.get("reason")
                            })
                            
                            if conf == "High": stats['high'] += 1
                            else: stats['low'] += 1
                            
                            time.sleep(1.5)
                            
                        ai_results_data.append(base_res)
                        prog.progress((i+1)/count_rem)
                        status.text(f"Processing ({i+1}/{count_rem}): {search_name}")
                        
                    except Exception as e:
                        st.error(f"Error at index {orig_idx}: {e}")
                        break
            
            if ai_results_data:
                df_ai_res = pd.DataFrame(ai_results_data)
                df_final = pd.concat([df_exact_pre, df_ai_res], ignore_index=True)
            else:
                df_final = df_exact_pre
            
            st.session_state.final_result_df = df_final
            st.session_state.match_stats = stats
            st.rerun()

# --- 4. 结果展示 ---
if st.session_state.final_result_df is not None:
    s = st.session_state.match_stats
    total = s.get('total', 1)
    if total == 0: total = 1
    
    st.markdown("### 📊 匹配统计报告")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown(f"""
        <div class="stat-card">
            <div class="sub-text">🎯 全字匹配</div>
            <div class="big-num">{s['exact']} 行</div>
            <div style="color:green; font-weight:bold;">{s['exact']/total:.1%}</div>
        </div>""", unsafe_allow_html=True)
    with col2:
        model_done = s['high'] + s['low']
        st.markdown(f"""
        <div class="stat-card">
            <div class="sub-text">🤖 模型处理</div>
            <div class="big-num">{model_done} 行</div>
            <div style="color:blue; font-weight:bold;">{model_done/total:.1%}</div>
        </div>""", unsafe_allow_html=True)
    with col3:
        st.markdown(f"""
        <div class="stat-card">
            <div class="sub-text">🔥 High 置信度</div>
            <div class="big-num">{s['high']} 行</div>
            <div class="sub-text">占模型: {s['high']/model_done:.1% if model_done else 0}</div>
        </div>""", unsafe_allow_html=True)
    with col4:
        st.markdown(f"""
        <div class="stat-card">
            <div class="sub-text">⚠️ Low 置信度</div>
            <div class="big-num">{s['low']} 行</div>
            <div class="sub-text">占模型: {s['low']/model_done:.1% if model_done else 0}</div>
        </div>""", unsafe_allow_html=True)

    st.divider()
    
    def color_row(row):
        if row['匹配方式'] == '全字匹配': return ['background-color: #dcfce7'] * len(row)
        if row.get('置信度') == 'High': return ['background-color: #e0f2fe'] * len(row)
        return [''] * len(row)

    df_show = st.session_state.final_result_df
    st.dataframe(df_show.style.apply(color_row, axis=1), use_container_width=True)
    
    csv = df_show.to_csv(index=False).encode('utf-8-sig')
    st.download_button("📥 下载完整报告", csv, "linkmed_expert_result.csv", "text/csv", type="primary")
