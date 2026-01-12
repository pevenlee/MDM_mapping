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
    """加载并建立多维索引 (安全版)"""
    if os.path.exists(LOCAL_MASTER_FILE):
        try:
            gc.collect()
            if LOCAL_MASTER_FILE.endswith('.xlsx'):
                df = pd.read_excel(LOCAL_MASTER_FILE, engine='openpyxl')
            else:
                df = pd.read_csv(LOCAL_MASTER_FILE)
            
            # 1. 索引重置 (防止索引混乱导致崩溃)
            df = df.reset_index(drop=True)
            
            # 2. 补全缺失列
            target_cols = ['标准名称', '省', '市', '区', '机构类型', '地址', '连锁品牌']
            for col in target_cols:
                if col not in df.columns:
                    df[col] = ''
            
            # 3. 强制类型转换
            for col in target_cols:
                df[col] = df[col].astype(str).replace('nan', '').str.strip()
                
            # 4. 建立索引
            geo_index = {
                'province': df.groupby('省').groups,
                'city': df.groupby('市').groups,
                'district': df.groupby('区').groups
            }
            
            chain_groups = {}
            mask = df['连锁品牌'].str.len() > 1
            if mask.any():
                chain_groups = df[mask].groupby('连锁品牌').groups
            
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

def get_candidates_hybrid_safe(search_name, chain_name, df_master, geo_index, chain_groups, user_row, mapping):
    """
    🌟 混合检索逻辑 (防崩溃版)
    """
    try:
        # 1. 确定地理范围索引
        u_prov = str(user_row[mapping['prov']]) if mapping['prov'] and pd.notna(user_row[mapping['prov']]) else ''
        u_city = str(user_row[mapping['city']]) if mapping['city'] and pd.notna(user_row[mapping['city']]) else ''
        u_dist = str(user_row[mapping['dist']]) if mapping['dist'] and pd.notna(user_row[mapping['dist']]) else ''
        
        geo_indices = set()
        scope_level = "Global"

        # 安全的字典查找 (用 .get 避免 KeyError)
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
            geo_indices = set(df_master.index)
            scope_level = "Global (No Geo Match)"

        candidates_indices = set()

        # 2. 策略 A: 连锁下钻
        if chain_name and chain_name in chain_groups:
            chain_store_indices = set(chain_groups[chain_name])
            valid_chain_stores = chain_store_indices.intersection(geo_indices)
            candidates_indices.update(valid_chain_stores)
            if len(valid_chain_stores) > 0:
                scope_level += " + Chain Drill-down"

        # 3. 策略 B: 模糊搜索
        if geo_indices:
            # 限制搜索范围，防止内存溢出
            search_pool_indices = list(geo_indices)
            
            # 使用 loc 安全提取
            current_scope_df = df_master.loc[search_pool_indices]
            choices = current_scope_df['标准名称'].fillna('').astype(str).to_dict()
            
            results = process.extract(search_name, choices, limit=5, scorer=fuzz.WRatio)
            for r in results:
                candidates_indices.add(r[2]) 

        return list(candidates_indices), scope_level
    
    except Exception as e:
        print(f"Retrieval Error: {e}")
        return [], "Error"

def ai_match_row_expert(client, user_row, search_name, chain_name, scope_level, candidates_df):
    cols_to_keep = ['esid', '标准名称', '机构类型', '省', '市', '区', '地址', '连锁品牌']
    valid_cols = [c for c in cols_to_keep if c in candidates_df.columns]
    candidates_json = candidates_df[valid_cols].to_json(orient="records", force_ascii=False)
    
    prompt = f"""
    【角色】主数据匹配专家。
    【输入】名称: "{search_name}", 连锁: "{chain_name}", 范围: {scope_level}
    【原始行】: {user_row.to_json(force_ascii=False)}
    【候选集】: {candidates_json}
    
    【核心规则】:
    1. **总部陷阱**: 除非输入明确是总部，否则不要匹配“总公司/总部”类型的候选。优先匹配门店。
    2. **地址交叉验证**: 输入名称若包含路名(如"人民路店")，请核对候选的【地址】列。地址吻合是最高置信度。
    3. **名称组合**: 若输入为"连锁+地名"，优先寻找名称或地址含该地名的记录。
    
    【输出 JSON】:
    {{ "match_esid": "...", "match_name": "...", "match_type": "...", "confidence": "High/Low", "reason": "..." }}
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
    🧬 LinkMed Matcher (Safe Mode)
    </div>
""", unsafe_allow_html=True)

client = get_client()

# 加载数据
df_master, geo_index, chain_groups = pd.DataFrame(), {}, {}
if os.path.exists(LOCAL_MASTER_FILE):
    with st.spinner(f"正在加载主数据..."):
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
        st.caption(f"连锁索引: {len(chain_groups)} 个品牌")

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
            col_chain = st.selectbox("🔗 连锁名称 (可选)", [None]+cols, index=cols.index(map_res['chain_col'])+1 if map_res.get('chain_col') in cols else 0)
        with c2:
            col_prov = st.selectbox("🗺️ 省份 (可选)", [None]+cols, index=cols.index(map_res['prov_col'])+1 if map_res.get('prov_col') in cols else 0)
            col_city = st.selectbox("🏙️ 城市 (可选)", [None]+cols, index=cols.index(map_res['city_col'])+1 if map_res.get('city_col') in cols else 0)
        with c3:
            col_dist = st.selectbox("🏘️ 区县 (可选)", [None]+cols, index=cols.index(map_res['dist_col'])+1 if map_res.get('dist_col') in cols else 0)
            col_addr = st.selectbox("🏠 详细地址 (可选)", [None]+cols, index=cols.index(map_res['addr_col'])+1 if map_res.get('addr_col') in cols else 0)

        mapping = {'prov': col_prov, 'city': col_city, 'dist': col_dist, 'addr': col_addr, 'chain': col_chain, 'name': col_name}

        # --- 3. 预处理分流 ---
        st.markdown("### ⚡ 3. 执行匹配")
        
        # 全字匹配字典
        master_exact = df_master.drop_duplicates(subset=['标准名称']).set_index('标准名称').to_dict('index')
        
        exact_rows = []
        rem_indices = []
        
        # 预扫描
        for idx, row in df_user.iterrows():
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
        df_rem = df_user.loc[rem_indices].copy()
        
        st.markdown(f"""
        <div class="success-box">✅ <b>已自动命中 {len(df_exact)} 行</b></div>
        <div class="info-box">⏳ <b>剩余 {len(df_rem)} 行</b> 待模型处理</div>
        """, unsafe_allow_html=True)
        
        btn_txt = f"🚀 开始处理剩余 {len(df_rem)} 行" if len(df_rem) > 0 else "✨ 生成结果"
        
        if st.button(btn_txt, type="primary"):
            ai_rows = []
            stats = {'exact': len(df_exact), 'high': 0, 'low': 0, 'no_match': 0}
            
            if len(df_rem) > 0:
                prog = st.progress(0)
                status = st.empty()
                
                for i, (orig_idx, row) in enumerate(df_rem.iterrows()):
                    try:
                        raw_name = str(row[col_name]).strip()
                        chain_name = str(row[col_chain]).strip() if col_chain and pd.notna(row[col_chain]) else ""
                        search_name = raw_name
                        if chain_name and chain_name not in raw_name: search_name = f"{chain_name} {raw_name}"

                        # 🌟 调用安全版检索
                        indices, scope = get_candidates_hybrid_safe(search_name, chain_name, df_master, geo_index, chain_groups, row, mapping)
                        
                        base_res = row.to_dict()
                        if not indices:
                            base_res.update({"匹配ESID": None, "匹配标准名": None, "机构类型": None, "置信度": "Low", "匹配方式": "无结果", "理由": "无候选"})
                            stats['no_match'] += 1
                        else:
                            try:
                                candidates = df_master.loc[indices].copy()
                            except:
                                candidates = pd.DataFrame()

                            if candidates.empty:
                                base_res.update({"匹配ESID": None, "匹配标准名": None, "机构类型": None, "置信度": "Low", "匹配方式": "无结果", "理由": "索引错误"})
                                stats['no_match'] += 1
                            else:
                                ai_res = ai_match_row_expert(client, row, search_name, chain_name, scope, candidates)
                                if isinstance(ai_res, list): ai_res = ai_res[0] if ai_res else {}
                                
                                conf = ai_res.get("confidence", "Low")
                                base_res.update({
                                    "匹配ESID": ai_res.get("match_esid"),
                                    "匹配标准名": ai_res.get("match_name"),
                                    "机构类型": ai_res.get("match_type"),
                                    "置信度": conf,
                                    "匹配方式": "模型匹配",
                                    "理由": ai_res.get("reason")
                                })
                                
                                if conf == "High": stats['high'] += 1
                                else: stats['low'] += 1
                                
                                time.sleep(1.5)
                        
                        ai_rows.append(base_res)
                        prog.progress((i+1)/len(df_rem))
                        status.text(f"Processing ({i+1}/{len(df_rem)}): {search_name}")
                        
                    except Exception as e:
                        st.warning(f"跳过行 {orig_idx}: {e}")
            
            # 合并结果
            if ai_rows:
                df_ai = pd.DataFrame(ai_rows)
                df_final = pd.concat([df_exact, df_ai], ignore_index=True)
            else:
                df_final = df_exact
            
            st.session_state.final_result_df = df_final
            st.session_state.match_stats = stats
            st.rerun()

# --- 4. 结果展示 ---
if st.session_state.final_result_df is not None:
    s = st.session_state.match_stats
    total = s.get('total', 0)
    # 如果统计数为0，尝试用df长度
    if total == 0: total = len(st.session_state.final_result_df)
    if total == 0: total = 1
    
    st.markdown("### 📊 匹配统计报告")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown(f"""
        <div class="stat-card">
            <div class="sub-text">🎯 全字匹配</div>
            <div class="big-num">{s.get('exact', 0)} 行</div>
            <div style="color:green; font-weight:bold;">{s.get('exact', 0)/total:.1%}</div>
        </div>""", unsafe_allow_html=True)
    with col2:
        model_done = s.get('high', 0) + s.get('low', 0)
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
            <div class="big-num">{s.get('high', 0)} 行</div>
            <div class="sub-text">占模型: {s.get('high', 0)/model_done:.1% if model_done else 0}</div>
        </div>""", unsafe_allow_html=True)
    with col4:
        st.markdown(f"""
        <div class="stat-card">
            <div class="sub-text">⚠️ Low 置信度</div>
            <div class="big-num">{s.get('low', 0)} 行</div>
            <div class="sub-text">占模型: {s.get('low', 0)/model_done:.1% if model_done else 0}</div>
        </div>""", unsafe_allow_html=True)

    st.divider()
    
    def color_row(row):
        if row.get('匹配方式') == '全字匹配': return ['background-color: #dcfce7'] * len(row)
        if row.get('置信度') == 'High': return ['background-color: #e0f2fe'] * len(row)
        return [''] * len(row)

    df_show = st.session_state.final_result_df
    st.dataframe(df_show.style.apply(color_row, axis=1), use_container_width=True)
    
    csv = df_show.to_csv(index=False).encode('utf-8-sig')
    st.download_button("📥 下载完整报告", csv, "linkmed_expert_result.csv", "text/csv", type="primary")
