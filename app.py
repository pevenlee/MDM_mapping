import streamlit as st
import pandas as pd
import json
import time
import os
import gc
import math
import re
from google import genai
from google.genai import types
from rapidfuzz import process, fuzz 

# ================= 1. 配置与初始化 =================

st.set_page_config(page_title="LinkMed Matcher Pro (Smart Logic)", layout="wide", page_icon="🧬")

try:
    FIXED_API_KEY = st.secrets["GENAI_API_KEY"]
except:
    FIXED_API_KEY = "" 

LOCAL_MASTER_FILE = "MDM_hospital.xlsx"

# --- Session State ---
if 'uploader_key' not in st.session_state: st.session_state.uploader_key = str(time.time())
if 'final_result_df' not in st.session_state: st.session_state.final_result_df = None
if 'match_stats' not in st.session_state: st.session_state.match_stats = {'exact': 0, 'high': 0, 'mid': 0, 'low': 0, 'no_match': 0}
# 任务流控制
if 'prep_done' not in st.session_state: st.session_state.prep_done = False
if 'mapping_confirmed' not in st.session_state: st.session_state.mapping_confirmed = False
if 'df_exact' not in st.session_state: st.session_state.df_exact = None
if 'batches' not in st.session_state: st.session_state.batches = []
if 'total_rem' not in st.session_state: st.session_state.total_rem = 0
if 'accumulated_results' not in st.session_state: st.session_state.accumulated_results = []
if 'is_running' not in st.session_state: st.session_state.is_running = False
if 'current_batch_idx' not in st.session_state: st.session_state.current_batch_idx = 0
if 'stop_requested' not in st.session_state: st.session_state.stop_requested = False

# ================= 2. 核心工具函数 =================

# --- 新增：核心词提取工具 ---
GENERIC_SUFFIXES = [
    '有限公司', '股份有限公司', '有限责任公司', '分店', '分公司', '药房', 
    '药店', '大药房', '大药店', '诊所', '卫生室', '卫生站', '服务站', 
    '医务室', '门诊部', '门诊', '医院', '中心', '总店', '旗舰店', '二店', '一店'
]

def extract_core_keywords(text):
    """
    1. 去除括号内容
    2. 去除通用后缀
    3. 如果剩余长度太短，则返回原词
    """
    if not isinstance(text, str): return ""
    
    # 1. 去除括号及内容 (e.g., "某某医院(总店)" -> "某某医院")
    text = re.sub(r"\(.*?\)|（.*?）|\[.*?\]|【.*?】", "", text)
    
    # 2. 循环去除后缀
    clean_text = text
    # 按长度降序排列后缀，优先匹配长的
    sorted_suffixes = sorted(GENERIC_SUFFIXES, key=len, reverse=True)
    
    for suffix in sorted_suffixes:
        if clean_text.endswith(suffix):
            # 只有当去除后缀后剩余长度 >= 2 才去除
            if len(clean_text) - len(suffix) >= 2:
                clean_text = clean_text[:-len(suffix)]
                
    return clean_text.strip()

def reset_app():
    """完全重置"""
    keys = ['final_result_df', 'match_stats', 'prep_done', 'mapping_confirmed', 'df_exact', 
            'batches', 'total_rem', 'is_running', 'current_batch_idx', 'accumulated_results', 'stop_requested']
    for k in keys:
        if k in st.session_state: del st.session_state[k]
    st.session_state.uploader_key = str(time.time())
    st.rerun()

def request_stop():
    """请求停止"""
    st.session_state.stop_requested = True
    st.session_state.is_running = False

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
                model="gemini-2.0-flash-exp", # 推荐使用更快的模型
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
    if os.path.exists(LOCAL_MASTER_FILE):
        try:
            gc.collect()
            if LOCAL_MASTER_FILE.endswith('.xlsx'):
                df = pd.read_excel(LOCAL_MASTER_FILE, engine='openpyxl')
            else:
                df = pd.read_csv(LOCAL_MASTER_FILE)
            
            df = df.reset_index(drop=True)
            # 确保主数据关键列存在
            target_cols = ['标准名称', '省', '市', '区', '机构类型', '地址']
            for col in target_cols:
                if col not in df.columns: df[col] = ''
                df[col] = df[col].astype(str).replace('nan', '').str.strip()
            
            # 建立多级索引
            prov_groups = df.groupby('省').groups
            city_groups = df.groupby('市').groups
            dist_groups = df.groupby('区').groups
            
            chain_groups = {}
            # 只有当 '连锁品牌' 列存在时才处理
            if '连锁品牌' in df.columns:
                mask = df['连锁品牌'].str.len() > 1
                if mask.any():
                    chain_groups = df[mask].groupby('连锁品牌').groups
            
            return df, prov_groups, city_groups, dist_groups, chain_groups
        except Exception as e:
            st.error(f"读取主数据错误: {e}")
            return pd.DataFrame(), {}, {}, {}, {}
    else:
        return pd.DataFrame(), {}, {}, {}, {}

def smart_map_columns(client, df_user):
    user_cols = df_user.columns.tolist()
    sample_data = df_user.head(3).to_markdown(index=False)
    # 优化Prompt：让AI更精准识别
    prompt = f"""
    分析医院数据表头。
    用户列名: {user_cols}
    预览: {sample_data}
    
    请推断以下字段对应哪一列（若无则null）：
    1. name_col: 医院名称 (核心)
    3. prov_col: 省份
    4. city_col: 城市
    5. dist_col: 区/县
    6. addr_col: 详细地址 (非常重要)
    
    输出 JSON: {{ "name_col": "...", "chain_col": "...", "prov_col": "...", "city_col": "...", "dist_col": "...", "addr_col": "..." }}
    """
    res = safe_generate(client, prompt)
    if isinstance(res, list): res = res[0] if res else {}
    return res

def get_candidates_hierarchical(search_name, chain_name, df_master, prov_groups, city_groups, dist_groups, chain_groups, user_row, mapping):
    """
    🌟 策略升级 V2 (核心词包含 + 宽范围)：
    1. 确定最宽的地理边界 (City > Prov > All)。
    2. 在地理边界内，进行【核心词包含搜索】 (Keyword Contains)。
    3. 结合原有的区县精准匹配。
    """
    try:
        u_prov = str(user_row[mapping['prov']]) if mapping['prov'] and pd.notna(user_row[mapping['prov']]) else ''
        u_city = str(user_row[mapping['city']]) if mapping['city'] and pd.notna(user_row[mapping['city']]) else ''
        u_dist = str(user_row[mapping['dist']]) if mapping['dist'] and pd.notna(user_row[mapping['dist']]) else ''
        
        candidates_indices = set()
        scope_indices = set() # 搜索底池
        scope_desc = ""

        # --- 1. 确定搜索底池 (Search Scope) ---
        if u_city and u_city in city_groups:
            scope_indices = set(city_groups[u_city])
            scope_desc = f"城市: {u_city}"
        elif u_prov and u_prov in prov_groups:
            scope_indices = set(prov_groups[u_prov])
            scope_desc = f"省份: {u_prov}"
        else:
            # 如果没有省市信息，底池就是全国 (注意性能)
            scope_indices = set(df_master.index)
            scope_desc = "全国范围"

        # --- 2. 核心词召回 (Keyword Recall) ---
        # 提取核心词，例如 "同仁堂药店" -> "同仁堂"
        core_word = extract_core_keywords(search_name)
        
        # 只有核心词有效且底池存在时
        if len(core_word) >= 2 and scope_indices:
            current_scope_list = list(scope_indices)
            
            # 如果是全国范围且数据量极大，这里为了性能可能需要限制，但为了查全率我们先不做硬限制
            # 优化：只在 Scope 范围内切片
            if len(current_scope_list) > 0:
                scope_df_slice = df_master.loc[current_scope_list]
                
                # 【关键一步】包含匹配：只要标准名称包含核心词，就拉进来
                keyword_mask = scope_df_slice['标准名称'].astype(str).str.contains(core_word, regex=False, na=False)
                keyword_indices = set(scope_df_slice[keyword_mask].index)
                
                candidates_indices.update(keyword_indices)
        
        # --- 3. 原有的地理层级补全 (Geographic Hierarchy) ---
        # 核心词可能提取不准，保留原有逻辑作为补充
        if u_dist and u_dist in dist_groups:
            dist_indices = set(dist_groups[u_dist])
            if u_city and u_city in city_groups:
                dist_indices = dist_indices.intersection(set(city_groups[u_city]))
            
            # 只有当候选池还很空的时候，才把整个区的医院都加进去
            if len(candidates_indices) < 5:
                candidates_indices.update(dist_indices)

        # --- 4. 连锁下钻 (Chain Drill-down) ---
        if chain_name and chain_name in chain_groups:
            chain_indices = set(chain_groups[chain_name])
            valid_chain = chain_indices.intersection(scope_indices)
            candidates_indices.update(valid_chain)

        # --- 5. 模糊搜索兜底 ---
        if len(candidates_indices) < 3 and list(scope_indices):
             # 限制搜索池大小
             search_pool_list = list(scope_indices)
             if len(search_pool_list) > 3000: search_pool_list = search_pool_list[:3000]
             
             choices = df_master.loc[search_pool_list, '标准名称'].fillna('').astype(str).to_dict()
             results = process.extract(search_name, choices, limit=3, scorer=fuzz.WRatio)
             for r in results:
                 candidates_indices.add(r[2])

        return list(candidates_indices), scope_desc
    
    except Exception as e:
        return [], "Error"

def ai_match_row_v4(client, user_row, search_name, chain_name, scope_desc, candidates_df):
    """
    🌟 V4 Prompt: 强化地址交叉验证与符号识别
    """
    cols_to_keep = ['esid', '标准名称', '机构类型', '省', '市', '区', '地址', '连锁品牌']
    valid_cols = [c for c in cols_to_keep if c in candidates_df.columns]
    candidates_json = candidates_df[valid_cols].to_json(orient="records", force_ascii=False)
    
    user_raw_addr = str(user_row.get('地址列_raw', ''))
    
    prompt = f"""
    【角色】你是一个极度严谨的主数据匹配专家。
    
    【待匹配目标】
    - 组合名称: "{search_name}"
    - 连锁品牌: "{chain_name}" (如果为空则无)
    - 所在区域: {scope_desc}
    - 原始地址: "{user_raw_addr}" (关键线索!)
    
    【候选主数据列表】(已限制在同区域内)
    {candidates_json}
    
    【思维链规则 - 必须严格执行】:
    1. **符号/短名识别**: 
       - 用户的名称可能极简，例如“一店”、“三分店”、“东门店”。
       - **必须**去候选数据的【标准名称】和**【地址】**中寻找包含这些关键词的记录。
       - 例如：用户输入“一店”，候选地址“XX路10号海王星辰第一分店”，这就是匹配！
       
    2. **地址交叉验证**: 
       - 如果名称匹配度不高，但【原始地址】与候选的【地址】高度吻合（如同路名、同门牌），则判定为 High。
       
    3. **连锁一致性**:
       - 如果用户指定了连锁品牌，候选必须属于该连锁（或名称包含该连锁）。
       - 严禁将A连锁的店匹配给B连锁。
       
    4. **总部陷阱**:
       - 除非用户找“总部”，否则不要匹配“总公司/股份有限公司”。请优先找具体的门店。
    
    【输出 JSON】:
    {{ 
      "match_esid": "ESID或null", 
      "match_name": "标准名称", 
      "match_type": "机构类型", 
      "confidence": "High/Mid/Low", 
      "reason": "说明匹配依据，如'地址路名完全一致'或'名称后缀匹配'" 
    }}
    """
    return safe_generate(client, prompt)

# ================= 3. 页面 UI =================

st.markdown("""
    <style>
    .stApp {background-color: #F8F9FA;}
    .stat-card {background: #ffffff; padding: 15px; border-radius: 8px; border: 1px solid #e5e7eb; box-shadow: 0 1px 2px rgba(0,0,0,0.05);}
    .big-num {font-size: 24px; font-weight: bold; color: #1e40af;}
    .task-box {background-color: #eff6ff; padding: 12px; border-radius: 6px; margin-bottom: 8px; border-left: 5px solid #2563eb; font-size:14px;}
    .running-box {background-color: #fff7ed; border: 2px solid #f97316; padding: 15px; border-radius: 8px;}
    </style>
    <div style="font-size: 26px; font-weight: bold; color: #1E3A8A; margin-bottom: 20px;">
    🧬 LinkMed Matcher (Smart Strategy)
    </div>
""", unsafe_allow_html=True)

client = get_client()

# 加载数据
df_master, prov_groups, city_groups, dist_groups, chain_groups = pd.DataFrame(), {}, {}, {}, {}
if os.path.exists(LOCAL_MASTER_FILE):
    with st.spinner(f"正在加载主数据引擎..."):
        df_master, prov_groups, city_groups, dist_groups, chain_groups = load_master_data()
else:
    st.warning(f"⚠️ 文件缺失: `{LOCAL_MASTER_FILE}`")

# --- Sidebar ---
with st.sidebar:
    st.header("🗄️ 控制台")
    
    # 🛑 停止按钮 (核心功能)
    if st.session_state.is_running:
        if st.button("🛑 停止并结算结果", type="primary", use_container_width=True):
            request_stop()
    else:
        if st.button("🗑️ 清空重置", type="secondary", use_container_width=True):
            reset_app()
            
    st.divider()
    if not df_master.empty:
        st.success(f"主数据: {len(df_master)} 条")

# --- 1. 上传与映射 ---
if not st.session_state.mapping_confirmed:
    st.markdown("### 📂 1. 上传与字段确认")
    uploaded_file = st.file_uploader("Excel/CSV", type=['xlsx', 'csv'], key=st.session_state.uploader_key)

    if uploaded_file and not df_master.empty:
        try:
            if uploaded_file.name.endswith('.csv'): df_user = pd.read_csv(uploaded_file)
            else: df_user = pd.read_excel(uploaded_file)
        except Exception as e:
            st.error(f"文件读取失败: {e}")
            st.stop()
        
        # 自动映射推断
        if 'map_config' not in st.session_state or st.session_state.get('last_file') != uploaded_file.name:
            with st.spinner("AI 正在识别表头..."):
                st.session_state.map_config = smart_map_columns(client, df_user)
                st.session_state.last_file = uploaded_file.name
        
        map_res = st.session_state.map_config
        cols = df_user.columns.tolist()
        
        st.info("👇 请务必确认 AI 识别的字段是否正确，如有误请手动修改：")
        
        c1, c2, c3 = st.columns(3)
        def get_idx(key): return cols.index(map_res.get(key)) if map_res.get(key) in cols else 0
        
        with c1:
            col_name = st.selectbox("📍 药房名称 (必选)", cols, index=get_idx('name_col'), help="也就是终端名称")
            col_chain = st.selectbox("🔗 连锁名称 (可选)", [None]+cols, index=cols.index(map_res['chain_col'])+1 if map_res.get('chain_col') in cols else 0)
        with c2:
            col_prov = st.selectbox("🗺️ 省份", [None]+cols, index=cols.index(map_res['prov_col'])+1 if map_res.get('prov_col') in cols else 0)
            col_city = st.selectbox("🏙️ 城市", [None]+cols, index=cols.index(map_res['city_col'])+1 if map_res.get('city_col') in cols else 0)
        with c3:
            col_dist = st.selectbox("🏘️ 区县", [None]+cols, index=cols.index(map_res['dist_col'])+1 if map_res.get('dist_col') in cols else 0)
            col_addr = st.selectbox("🏠 详细地址 (重要)", [None]+cols, index=cols.index(map_res['addr_col'])+1 if map_res.get('addr_col') in cols else 0)

        if st.button("✅ 确认字段映射并继续", type="primary"):
            st.session_state.user_mapping = {
                'prov': col_prov, 'city': col_city, 'dist': col_dist, 
                'addr': col_addr, 'chain': col_chain, 'name': col_name
            }
            # 存入原始数据备用
            st.session_state.raw_df_user = df_user
            st.session_state.mapping_confirmed = True
            st.rerun()

# --- 2. 预处理与分包 ---
elif st.session_state.mapping_confirmed and not st.session_state.prep_done:
    st.markdown("### ⚡ 2. 预处理分析")
    
    mapping = st.session_state.user_mapping
    df_user = st.session_state.raw_df_user
    
    with st.spinner("正在进行数据清洗、排序与全字匹配..."):
        try:
            # 1. 安全清洗
            df_safe = df_user.copy()
            for k, c in mapping.items():
                if c: df_safe[c] = df_safe[c].astype(str).replace('nan', '').str.strip()
            
            # 2. 地理排序 (让同区数据在一起)
            sort_cols = []
            if mapping['prov']: sort_cols.append(mapping['prov'])
            if mapping['city']: sort_cols.append(mapping['city'])
            if mapping['dist']: sort_cols.append(mapping['dist'])
            if sort_cols:
                df_safe = df_safe.sort_values(by=sort_cols).reset_index(drop=True)

            # 3. 向量化全字匹配 (新版：全库匹配)
            # 直接建立 {标准名称: Metadata} 字典，不考虑地理因素
            master_exact = df_master.drop_duplicates(subset=['标准名称']).set_index('标准名称').to_dict('index')
            
            def check_exact(row):
                raw_name = str(row[mapping['name']]).strip()
                # 只有全库完全一致才算 Exact Match
                if raw_name in master_exact:
                    m = master_exact[raw_name]
                    return pd.Series([True, m.get('esid'), raw_name, m.get('机构类型'), "High", "全字匹配", "全库精确命中"])
                
                return pd.Series([False, None, None, None, None, None, None])

            match_results = df_safe.apply(check_exact, axis=1)
            match_results.columns = ['is_match', '匹配ESID', '匹配标准名', '机构类型', '置信度', '匹配方式', '理由']
            
            df_combined = pd.concat([df_safe, match_results], axis=1)
            
            # 拆分
            df_exact = df_combined[df_combined['is_match'] == True].drop(columns=['is_match'])
            df_rem = df_combined[df_combined['is_match'] == False].drop(columns=['is_match', '匹配ESID', '匹配标准名', '机构类型', '置信度', '匹配方式', '理由'])
            
            st.session_state.df_exact = df_exact
            st.session_state.total_rem = len(df_rem)
            
            # 拆任务包 (每包 800 条，稍微小一点防超时)
            BATCH_SIZE = 800
            batches = []
            if len(df_rem) > 0:
                num_batches = math.ceil(len(df_rem) / BATCH_SIZE)
                for i in range(num_batches):
                    batches.append(df_rem.iloc[i*BATCH_SIZE : (i+1)*BATCH_SIZE])
            
            st.session_state.batches = batches
            st.session_state.prep_done = True
            st.session_state.match_stats['exact'] = len(df_exact)
            st.rerun()
            
        except Exception as e:
            st.error(f"预处理错误: {e}")
            st.stop()

# --- 3. 任务执行与监控 ---
elif st.session_state.prep_done and not st.session_state.final_result_df is not None:
    # 还没点开始
    if not st.session_state.is_running and len(st.session_state.accumulated_results) == 0:
        count_exact = len(st.session_state.df_exact)
        count_rem = st.session_state.total_rem
        batches = st.session_state.batches
        
        st.info(f"✅ 自动命中 {count_exact} 行。剩余 {count_rem} 行待 AI 深度匹配。")
        
        if count_rem > 0:
            st.markdown(f"**已拆分为 {len(batches)} 个任务包，点击启动后将自动接力执行。**")
            if st.button(f"🚀 启动深度匹配 ({len(batches)} 包)", type="primary"):
                st.session_state.is_running = True
                st.session_state.current_batch_idx = 0
                st.session_state.stop_requested = False
                st.rerun()
        else:
            if st.button("✨ 直接生成结果", type="primary"):
                st.session_state.final_result_df = st.session_state.df_exact
                st.rerun()

    # 正在运行中 (Relay Loop)
    elif st.session_state.is_running:
        
        batches = st.session_state.batches
        curr_idx = st.session_state.current_batch_idx
        mapping = st.session_state.user_mapping
        
        if curr_idx < len(batches):
            current_batch = batches[curr_idx]
            batch_num = curr_idx + 1
            
            st.markdown(f"""
            <div class='running-box'>
                <h3>🔄 正在处理任务包 {batch_num} / {len(batches)}</h3>
                <p>当前包包含 {len(current_batch)} 行数据。<b>点击左侧“🛑 停止并结算”可随时中断保存。</b></p>
            </div>
            """, unsafe_allow_html=True)
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            batch_results = []
            
            for i, (orig_idx, row) in enumerate(current_batch.iterrows()):
                
                # 🔥 检查停止信号
                if st.session_state.stop_requested:
                    break
                
                try:
                    # 准备数据
                    raw_name = str(row[mapping['name']])
                    chain_name = str(row[mapping['chain']]) if mapping['chain'] else ""
                    search_name = raw_name
                    if chain_name and chain_name not in raw_name: search_name = f"{chain_name} {raw_name}"
                    
                    row_with_meta = row.copy()
                    if mapping['addr']: row_with_meta['地址列_raw'] = str(row[mapping['addr']])

                    # 1. 策略升级：V2 核心词包含检索
                    indices, scope_desc = get_candidates_hierarchical(
                        search_name, chain_name, df_master, 
                        prov_groups, city_groups, dist_groups, chain_groups, 
                        row, mapping
                    )
                    
                    base_res = row.to_dict()
                    
                    if not indices:
                        base_res.update({"匹配ESID": None, "匹配标准名": None, "机构类型": None, "置信度": "Low", "匹配方式": "无结果", "理由": f"区域[{scope_desc}]无匹配"})
                        st.session_state.match_stats['no_match'] += 1
                    else:
                        try:
                            candidates = df_master.loc[indices].copy()
                        except:
                            candidates = pd.DataFrame()

                        if candidates.empty:
                            base_res.update({"匹配ESID": None, "匹配标准名": None, "机构类型": None, "置信度": "Low", "匹配方式": "无结果", "理由": "索引异常"})
                            st.session_state.match_stats['no_match'] += 1
                        else:
                            # 2. V4 Prompt 
                            ai_res = ai_match_row_v4(client, row_with_meta, search_name, chain_name, scope_desc, candidates)
                            if isinstance(ai_res, list): ai_res = ai_res[0] if ai_res else {}
                            
                            conf = ai_res.get("confidence", "Low")
                            base_res.update({
                                "匹配ESID": ai_res.get("match_esid"),
                                "匹配标准名": ai_res.get("match_name"),
                                "机构类型": ai_res.get("match_type"),
                                "置信度": conf,
                                "匹配方式": f"模型 ({scope_desc})",
                                "理由": ai_res.get("reason")
                            })
                            
                            if conf == "High": st.session_state.match_stats['high'] += 1
                            elif conf == "Mid": st.session_state.match_stats['mid'] += 1
                            else: st.session_state.match_stats['low'] += 1
                            
                            time.sleep(1.5) # 冷却
                    
                    batch_results.append(base_res)
                    progress_bar.progress((i + 1) / len(current_batch))
                    status_text.caption(f"正在匹配: {search_name}")
                    
                except Exception as e:
                    pass
            
            # --- Batch End ---
            # 存入总池
            st.session_state.accumulated_results.extend(batch_results)
            
            # 如果是点了停止
            if st.session_state.stop_requested:
                st.warning("🛑 任务已停止。正在生成已完成部分的报告...")
                st.session_state.is_running = False
                # 触发合并
                df_exact = st.session_state.df_exact
                df_ai = pd.DataFrame(st.session_state.accumulated_results)
                final = pd.concat([df_exact, df_ai], ignore_index=True) if not df_ai.empty else df_exact
                st.session_state.final_result_df = final
                st.rerun()
            else:
                # 正常完成一个包
                st.session_state.current_batch_idx += 1
                st.rerun()
        
        else:
            # 全部包跑完
            st.success("🎉 全部任务完成！")
            st.session_state.is_running = False
            df_exact = st.session_state.df_exact
            df_ai = pd.DataFrame(st.session_state.accumulated_results)
            final = pd.concat([df_exact, df_ai], ignore_index=True) if not df_ai.empty else df_exact
            st.session_state.final_result_df = final
            st.rerun()

# --- 4. 结果展示 ---
if st.session_state.final_result_df is not None:
    s = st.session_state.match_stats
    total = len(st.session_state.final_result_df)
    if total == 0: total = 1
    
    st.markdown("### 📊 匹配统计报告")
    
    exact_val = s.get('exact', 0)
    exact_pct = exact_val / total
    
    # 动态计算模型已跑的数量
    model_done = s.get('high', 0) + s.get('mid', 0) + s.get('low', 0)
    model_denom = model_done if model_done > 0 else 1
    
    # 显示统计卡片
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1: st.metric("🎯 全字匹配", f"{exact_val}", f"{exact_pct:.1%}")
    with c2: st.metric("🤖 模型已跑", f"{model_done}", f"{(model_done/(st.session_state.total_rem if st.session_state.total_rem else 1)):.1%}")
    with c3: st.metric("🔥 High", f"{s.get('high', 0)}", f"{s.get('high', 0)/model_denom:.1%}")
    with c4: st.metric("⚖️ Mid", f"{s.get('mid', 0)}", f"{s.get('mid', 0)/model_denom:.1%}")
    with c5: st.metric("⚠️ Low", f"{s.get('low', 0)}", f"{s.get('low', 0)/model_denom:.1%}")

    st.divider()
    
    def color_row(row):
        conf = row.get('置信度')
        if conf == 'High': return ['background-color: #dcfce7'] * len(row)
        if conf == 'Mid': return ['background-color: #fef9c3'] * len(row)
        if conf == 'Low': return ['background-color: #fee2e2'] * len(row)
        return [''] * len(row)

    df_show = st.session_state.final_result_df
    st.dataframe(df_show.style.apply(color_row, axis=1), use_container_width=True)
    
    csv = df_show.to_csv(index=False).encode('utf-8-sig')
    st.download_button("📥 下载结果文件", csv, "linkmed_final_result.csv", "text/csv", type="primary")
