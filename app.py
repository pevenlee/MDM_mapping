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

st.set_page_config(page_title="LinkMed Matcher Hierarchical", layout="wide", page_icon="🧬")

try:
    FIXED_API_KEY = st.secrets["GENAI_API_KEY"]
except:
    FIXED_API_KEY = "" 

LOCAL_MASTER_FILE = "MDM_retail.xlsx"

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
    """加载并建立严格的地理分层索引"""
    if os.path.exists(LOCAL_MASTER_FILE):
        try:
            gc.collect()
            if LOCAL_MASTER_FILE.endswith('.xlsx'):
                df = pd.read_excel(LOCAL_MASTER_FILE, engine='openpyxl')
            else:
                df = pd.read_csv(LOCAL_MASTER_FILE)
            
            # 1. 索引重置
            df = df.reset_index(drop=True)
            
            # 2. 补全并清洗列
            target_cols = ['标准名称', '省', '市', '区', '机构类型', '地址', '连锁品牌']
            for col in target_cols:
                if col not in df.columns: df[col] = ''
                df[col] = df[col].astype(str).replace('nan', '').str.strip()
                
            # 3. 建立分层索引
            prov_groups = df.groupby('省').groups
            city_groups = df.groupby('市').groups
            dist_groups = df.groupby('区').groups
            
            chain_groups = {}
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
    prompt = f"""
    分析用户数据，找出以下字段对应的列名。
    用户列名: {user_cols}
    预览: {sample_data}
    任务：找出以下列（无则null）：
    1. name_col: 药房名称
    2. chain_col: 连锁/品牌名称
    3. prov_col: 省份
    4. city_col: 城市
    5. dist_col: 区/县
    6. addr_col: 详细地址
    输出 JSON: {{ "name_col": "...", "chain_col": "...", "prov_col": "...", "city_col": "...", "dist_col": "...", "addr_col": "..." }}
    """
    res = safe_generate(client, prompt)
    if isinstance(res, list): res = res[0] if res else {}
    return res

def get_candidates_hierarchical(search_name, chain_name, df_master, prov_groups, city_groups, dist_groups, chain_groups, user_row, mapping):
    """
    🌟 严格分层检索逻辑 (Hierarchical Scope)
    优先级: 区 > 市 > 省 > 全局
    """
    try:
        # 获取用户行数据
        u_prov = str(user_row[mapping['prov']]) if mapping['prov'] and pd.notna(user_row[mapping['prov']]) else ''
        u_city = str(user_row[mapping['city']]) if mapping['city'] and pd.notna(user_row[mapping['city']]) else ''
        u_dist = str(user_row[mapping['dist']]) if mapping['dist'] and pd.notna(user_row[mapping['dist']]) else ''
        
        target_indices = set()
        scope_desc = ""

        # --- 层级 1: 区匹配 ---
        if u_dist and u_dist in dist_groups:
            dist_indices = set(dist_groups[u_dist])
            if u_city and u_city in city_groups:
                city_indices = set(city_groups[u_city])
                intersection = dist_indices.intersection(city_indices)
                if intersection:
                    target_indices = intersection
                    scope_desc = f"精准定位: {u_city}{u_dist}"
                else:
                    target_indices = dist_indices
                    scope_desc = f"区域定位: {u_dist}"
            else:
                target_indices = dist_indices
                scope_desc = f"区域定位: {u_dist}"
        
        # --- 层级 2: 市匹配 ---
        elif u_city and u_city in city_groups:
            target_indices = set(city_groups[u_city])
            scope_desc = f"城市定位: {u_city}"
            
        # --- 层级 3: 省匹配 ---
        elif u_prov and u_prov in prov_groups:
            target_indices = set(prov_groups[u_prov])
            scope_desc = f"省份定位: {u_prov}"
            
        # --- 层级 4: 全局 ---
        else:
            target_indices = set(df_master.index)
            scope_desc = "全局搜索"

        # --- 连锁下钻增强 ---
        force_chain_indices = set()
        if chain_name and chain_name in chain_groups:
            chain_indices = set(chain_groups[chain_name])
            force_chain_indices = chain_indices.intersection(target_indices)

        candidates_indices = set()
        candidates_indices.update(force_chain_indices) 
        
        if target_indices:
            search_pool_indices = list(target_indices)
            # 安全切片：如果范围太大且已有连锁候选，减少模糊搜索量
            if len(search_pool_indices) > 5000 and len(force_chain_indices) > 0:
                 search_pool_indices = search_pool_indices[:2000] # 采样防止超时

            current_scope_df = df_master.loc[search_pool_indices]
            choices = current_scope_df['标准名称'].fillna('').astype(str).to_dict()
            
            results = process.extract(search_name, choices, limit=8, scorer=fuzz.WRatio)
            for r in results:
                candidates_indices.add(r[2])

        return list(candidates_indices), scope_desc
    
    except Exception as e:
        print(f"Hierarchical Retrieval Error: {e}")
        return [], "Error"

def ai_match_row_v3(client, user_row, search_name, chain_name, scope_desc, candidates_df):
    cols_to_keep = ['esid', '标准名称', '机构类型', '省', '市', '区', '地址', '连锁品牌']
    valid_cols = [c for c in cols_to_keep if c in candidates_df.columns]
    candidates_json = candidates_df[valid_cols].to_json(orient="records", force_ascii=False)
    
    prompt = f"""
    【角色】主数据匹配专家。
    【待匹配实体】
    - 组合名称: "{search_name}"
    - 连锁品牌: "{chain_name}"
    - 检索范围: {scope_desc}
    - 原始地址: "{user_row.get('地址列_raw', '')}"
    
    【候选主数据】
    {candidates_json}
    
    【匹配标准 - 分级置信度】:
    1. **High**: 核心名称一致且地址/行政区吻合。
    2. **Mid**: 是同一连锁，但分店名有细微差异(如"一分店"vs"一店")，或地址缺失但区域内仅此一家。
    3. **Low**: 名称相似无法确定，或只有连锁名一致分店不同。
       
    【特殊规则】
    - **总部陷阱**: 除非用户找总部，否则不要匹配"总公司"。优先匹配门店。
    
    【输出 JSON】:
    {{ "match_esid": "...", "match_name": "...", "match_type": "...", "confidence": "High/Mid/Low", "reason": "..." }}
    """
    return safe_generate(client, prompt)

# ================= 3. 页面 UI =================

st.markdown("""
    <style>
    .stApp {background-color: #F8F9FA;}
    .stat-card {background: #ffffff; padding: 15px; border-radius: 8px; border: 1px solid #e5e7eb; box-shadow: 0 1px 2px rgba(0,0,0,0.05);}
    .big-num {font-size: 24px; font-weight: bold; color: #1e40af;}
    .sub-text {font-size: 14px; color: #6b7280;}
    </style>
    <div style="font-size: 26px; font-weight: bold; color: #1E3A8A; margin-bottom: 20px;">
    🧬 LinkMed Matcher (Hierarchical Logic)
    </div>
""", unsafe_allow_html=True)

client = get_client()

# 加载数据
df_master, prov_groups, city_groups, dist_groups, chain_groups = pd.DataFrame(), {}, {}, {}, {}
if os.path.exists(LOCAL_MASTER_FILE):
    with st.spinner(f"正在构建分层地理索引..."):
        df_master, prov_groups, city_groups, dist_groups, chain_groups = load_master_data()
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
        try:
            if uploaded_file.name.endswith('.csv'): df_user = pd.read_csv(uploaded_file)
            else: df_user = pd.read_excel(uploaded_file)
        except Exception as e:
            st.error(f"文件读取失败: {e}")
            st.stop()
        
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
            col_chain = st.selectbox("🔗 连锁名称", [None]+cols, index=cols.index(map_res['chain_col'])+1 if map_res.get('chain_col') in cols else 0)
        with c2:
            col_prov = st.selectbox("🗺️ 省份", [None]+cols, index=cols.index(map_res['prov_col'])+1 if map_res.get('prov_col') in cols else 0)
            col_city = st.selectbox("🏙️ 城市", [None]+cols, index=cols.index(map_res['city_col'])+1 if map_res.get('city_col') in cols else 0)
        with c3:
            col_dist = st.selectbox("🏘️ 区县", [None]+cols, index=cols.index(map_res['dist_col'])+1 if map_res.get('dist_col') in cols else 0)
            col_addr = st.selectbox("🏠 详细地址", [None]+cols, index=cols.index(map_res['addr_col'])+1 if map_res.get('addr_col') in cols else 0)

        mapping = {'prov': col_prov, 'city': col_city, 'dist': col_dist, 'addr': col_addr, 'chain': col_chain, 'name': col_name}

        # --- 3. 预处理与重排 ---
        st.markdown("### ⚡ 3. 分组重排与匹配")
        
        # 分组重排
        sort_cols = []
        if col_prov: sort_cols.append(col_prov)
        if col_city: sort_cols.append(col_city)
        if col_dist: sort_cols.append(col_dist)
        
        if sort_cols:
            df_user_sorted = df_user.sort_values(by=sort_cols).reset_index(drop=True)
            st.caption(f"✅ 已按 {sort_cols} 对数据进行分组重排，将按区域逐块匹配。")
        else:
            df_user_sorted = df_user

        # 全字匹配准备
        master_exact = df_master.drop_duplicates(subset=['标准名称']).set_index('标准名称').to_dict('index')
        
        exact_rows = []
        rem_indices = []
        
        # 预扫描
        for idx, row in df_user_sorted.iterrows():
            raw_name = str(row[col_name]).strip()
            chain_name = str(row[col_chain]).strip() if col_chain and pd.notna(row[col_chain]) else ""
            search_name = raw_name
            if chain_name and chain_name not in raw_name: search_name = f"{chain_name} {raw_name}"
            
            if search_name in master_exact:
                m = master_exact[search_name]
                r = row.to_dict()
                r.update({"匹配ESID": m.get('esid'), "匹配标准名": search_name, "机构类型": m.get('机构类型'), "置信度": "High", "匹配方式": "全字匹配", "理由": "精确命中"})
                exact_rows.append(r)
            else:
                rem_indices.append(idx)
        
        df_exact = pd.DataFrame(exact_rows)
        df_rem = df_user_sorted.loc[rem_indices].copy()
        
        st.info(f"预处理完成：自动命中 {len(df_exact)} 行，剩余 {len(df_rem)} 行待分层模型匹配。")
        
        btn_txt = f"🚀 开始分层匹配 ({len(df_rem)} 行)" if len(df_rem) > 0 else "✨ 生成结果"
        
        if st.button(btn_txt, type="primary"):
            ai_rows = []
            stats = {'exact': len(df_exact), 'high': 0, 'mid': 0, 'low': 0, 'no_match': 0}
            
            if len(df_rem) > 0:
                prog = st.progress(0)
                status = st.empty()
                
                for i, (orig_idx, row) in enumerate(df_rem.iterrows()):
                    try:
                        raw_name = str(row[col_name]).strip()
                        chain_name = str(row[col_chain]).strip() if col_chain and pd.notna(row[col_chain]) else ""
                        search_name = raw_name
                        if chain_name and chain_name not in raw_name: search_name = f"{chain_name} {raw_name}"
                        
                        row_with_meta = row.copy()
                        if col_addr: row_with_meta['地址列_raw'] = str(row[col_addr])

                        indices, scope_desc = get_candidates_hierarchical(
                            search_name, chain_name, df_master, 
                            prov_groups, city_groups, dist_groups, chain_groups, 
                            row, mapping
                        )
                        
                        base_res = row.to_dict()
                        if not indices:
                            base_res.update({"匹配ESID": None, "匹配标准名": None, "机构类型": None, "置信度": "Low", "匹配方式": "无结果", "理由": f"范围[{scope_desc}]内无候选"})
                            stats['no_match'] += 1
                        else:
                            try:
                                candidates = df_master.loc[indices].copy()
                            except:
                                candidates = pd.DataFrame()

                            if candidates.empty:
                                base_res.update({"匹配ESID": None, "匹配标准名": None, "机构类型": None, "置信度": "Low", "匹配方式": "无结果", "理由": "索引异常"})
                                stats['no_match'] += 1
                            else:
                                ai_res = ai_match_row_v3(client, row_with_meta, search_name, chain_name, scope_desc, candidates)
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
                                
                                if conf == "High": stats['high'] += 1
                                elif conf == "Mid": stats['mid'] += 1
                                else: stats['low'] += 1
                                
                                time.sleep(1.5)
                        
                        ai_rows.append(base_res)
                        prog.progress((i+1)/len(df_rem))
                        status.text(f"[{scope_desc}] Processing: {search_name}")
                        
                    except Exception as e:
                        st.warning(f"跳过行: {e}")
            
            if ai_rows:
                df_ai = pd.DataFrame(ai_rows)
                df_final = pd.concat([df_exact, df_ai], ignore_index=True)
            else:
                df_final = df_exact
            
            st.session_state.final_result_df = df_final
            st.session_state.match_stats = stats
            st.rerun()

# --- 4. 结果展示 (已修复 ValueError) ---
if st.session_state.final_result_df is not None:
    s = st.session_state.match_stats
    total = s.get('total', 0)
    if total == 0: total = len(st.session_state.final_result_df)
    if total == 0: total = 1
    
    st.markdown("### 📊 匹配统计报告")
    
    # 提前计算比率，避免 ValueError
    exact_val = s.get('exact', 0)
    exact_pct = exact_val / total
    
    model_done = s.get('high', 0) + s.get('mid', 0) + s.get('low', 0)
    model_pct = model_done / total
    
    model_denom = model_done if model_done > 0 else 1
    high_pct = s.get('high', 0) / model_denom
    mid_pct = s.get('mid', 0) / model_denom
    low_pct = s.get('low', 0) / model_denom
    
    c1, c2, c3, c4, c5 = st.columns(5)
    
    with c1:
        st.markdown(f"""
        <div class="stat-card">
            <div class="sub-text">🎯 全字匹配</div>
            <div class="big-num">{exact_val}</div>
            <div style="color:green; font-weight:bold;">{exact_pct:.1%}</div>
        </div>""", unsafe_allow_html=True)
    with c2:
        st.markdown(f"""
        <div class="stat-card">
            <div class="sub-text">🤖 模型总计</div>
            <div class="big-num">{model_done}</div>
            <div style="color:blue; font-weight:bold;">{model_pct:.1%}</div>
        </div>""", unsafe_allow_html=True)
    with c3:
        st.markdown(f"""
        <div class="stat-card">
            <div class="sub-text">🔥 High</div>
            <div class="big-num">{s.get('high', 0)}</div>
            <div class="sub-text">占模型: {high_pct:.1%}</div>
        </div>""", unsafe_allow_html=True)
    with c4:
        st.markdown(f"""
        <div class="stat-card">
            <div class="sub-text">⚖️ Mid</div>
            <div class="big-num">{s.get('mid', 0)}</div>
            <div class="sub-text">占模型: {mid_pct:.1%}</div>
        </div>""", unsafe_allow_html=True)
    with c5:
        st.markdown(f"""
        <div class="stat-card">
            <div class="sub-text">⚠️ Low</div>
            <div class="big-num">{s.get('low', 0)}</div>
            <div class="sub-text">占模型: {low_pct:.1%}</div>
        </div>""", unsafe_allow_html=True)

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
    st.download_button("📥 下载结果 (含 High/Mid/Low)", csv, "linkmed_hierarchical.csv", "text/csv", type="primary")
