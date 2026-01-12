import streamlit as st
import pandas as pd
import json
import time
import os
import gc
import math
from google import genai
from google.genai import types
from rapidfuzz import process, fuzz 

# ================= 1. 配置与初始化 =================

st.set_page_config(page_title="LinkMed Matcher Pro (Clean)", layout="wide", page_icon="🧬")

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
if 'batch_progress' not in st.session_state:
    st.session_state.batch_progress = [] 

# ================= 2. 核心工具函数 =================

def reset_app():
    st.session_state.final_result_df = None
    st.session_state.match_stats = {}
    st.session_state.batch_progress = []
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
    """
    标准加载模式：每次直接读取 Excel/CSV，不使用 Pickle 缓存
    确保数据 100% 准确，无缓存干扰
    """
    if os.path.exists(LOCAL_MASTER_FILE):
        try:
            gc.collect()
            # 根据后缀读取
            if LOCAL_MASTER_FILE.endswith('.xlsx'):
                df = pd.read_excel(LOCAL_MASTER_FILE, engine='openpyxl')
            else:
                df = pd.read_csv(LOCAL_MASTER_FILE)
            
            # 基础清洗
            df = df.reset_index(drop=True)
            target_cols = ['标准名称', '省', '市', '区', '机构类型', '地址', '连锁品牌']
            for col in target_cols:
                if col not in df.columns: df[col] = ''
                df[col] = df[col].astype(str).replace('nan', '').str.strip()
            
            # 建立索引
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
    try:
        u_prov = str(user_row[mapping['prov']]) if mapping['prov'] and pd.notna(user_row[mapping['prov']]) else ''
        u_city = str(user_row[mapping['city']]) if mapping['city'] and pd.notna(user_row[mapping['city']]) else ''
        u_dist = str(user_row[mapping['dist']]) if mapping['dist'] and pd.notna(user_row[mapping['dist']]) else ''
        
        target_indices = set()
        scope_desc = ""

        if u_dist and u_dist in dist_groups:
            dist_indices = set(dist_groups[u_dist])
            if u_city and u_city in city_groups:
                city_indices = set(city_groups[u_city])
                intersection = dist_indices.intersection(city_indices)
                target_indices = intersection if intersection else dist_indices
                scope_desc = f"精准定位: {u_city}{u_dist}"
            else:
                target_indices = dist_indices
                scope_desc = f"区域定位: {u_dist}"
        
        elif u_city and u_city in city_groups:
            target_indices = set(city_groups[u_city])
            scope_desc = f"城市定位: {u_city}"
            
        elif u_prov and u_prov in prov_groups:
            target_indices = set(prov_groups[u_prov])
            scope_desc = f"省份定位: {u_prov}"
            
        else:
            target_indices = set(df_master.index)
            scope_desc = "全局搜索"

        force_chain_indices = set()
        if chain_name and chain_name in chain_groups:
            chain_indices = set(chain_groups[chain_name])
            force_chain_indices = chain_indices.intersection(target_indices)

        candidates_indices = set()
        candidates_indices.update(force_chain_indices) 
        
        if target_indices:
            search_pool_indices = list(target_indices)
            if len(search_pool_indices) > 5000 and len(force_chain_indices) > 0:
                 search_pool_indices = search_pool_indices[:2000] 

            current_scope_df = df_master.loc[search_pool_indices]
            choices = current_scope_df['标准名称'].fillna('').astype(str).to_dict()
            
            results = process.extract(search_name, choices, limit=8, scorer=fuzz.WRatio)
            for r in results:
                candidates_indices.add(r[2])

        return list(candidates_indices), scope_desc
    
    except Exception as e:
        print(f"Retrieval Error: {e}")
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
    2. **Mid**: 是同一连锁，但分店名有细微差异，或地址缺失但区域内仅此一家。
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
    .task-box {background-color: #f3f4f6; padding: 10px; border-radius: 5px; margin-bottom: 5px; border-left: 4px solid #3b82f6;}
    .prog-label {font-weight: bold; font-size: 14px; margin-bottom: 5px; display: block;}
    </style>
    <div style="font-size: 26px; font-weight: bold; color: #1E3A8A; margin-bottom: 20px;">
    🧬 LinkMed Matcher (Clean Mode)
    </div>
""", unsafe_allow_html=True)

client = get_client()

# 加载数据
df_master, prov_groups, city_groups, dist_groups, chain_groups = pd.DataFrame(), {}, {}, {}, {}
if os.path.exists(LOCAL_MASTER_FILE):
    with st.spinner(f"正在加载主数据 (实时读取)..."):
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

        # --- 3. 预处理与分包 ---
        st.markdown("### ⚡ 3. 预处理与分包")
        
        # 分组重排
        sort_cols = []
        if col_prov: sort_cols.append(col_prov)
        if col_city: sort_cols.append(col_city)
        if col_dist: sort_cols.append(col_dist)
        
        if sort_cols:
            df_user_sorted = df_user.sort_values(by=sort_cols).reset_index(drop=True)
            st.caption(f"✅ 已按地理位置重排数据，优化匹配效率。")
        else:
            df_user_sorted = df_user

        # 全字匹配
        master_exact = df_master.drop_duplicates(subset=['标准名称']).set_index('标准名称').to_dict('index')
        exact_rows = []
        rem_indices = []
        
        with st.spinner("正在进行全字匹配预筛选..."):
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
        df_rem = df_user_sorted.loc[rem_indices].copy().reset_index(drop=True)
        
        # 拆包逻辑
        BATCH_SIZE = 2000
        num_batches = 1
        batches = []
        
        if len(df_rem) > 0:
            num_batches = math.ceil(len(df_rem) / BATCH_SIZE)
            for i in range(num_batches):
                batches.append(df_rem.iloc[i*BATCH_SIZE : (i+1)*BATCH_SIZE])

        st.info(f"预处理报告: 自动命中 {len(df_exact)} 行。剩余 {len(df_rem)} 行待模型匹配。")
        
        if len(df_rem) > 0:
            st.warning(f"由于数据量较大，已自动拆分为 **{num_batches}** 个任务包。")
            
            # 显示双进度条占位
            if st.button(f"🚀 启动任务队列 ({len(df_rem)} 行)", type="primary"):
                
                final_accumulated = df_exact.copy() if not df_exact.empty else pd.DataFrame()
                stats = {'exact': len(df_exact), 'high': 0, 'mid': 0, 'low': 0, 'no_match': 0}
                
                st.write("") 
                col_g, col_b = st.columns(2)
                with col_g:
                    st.markdown('<span class="prog-label">🌍 全局总进度</span>', unsafe_allow_html=True)
                    global_prog = st.progress(0)
                    global_txt = st.empty()
                
                with col_b:
                    st.markdown('<span class="prog-label">📦 当前任务包进度</span>', unsafe_allow_html=True)
                    batch_prog = st.progress(0)
                    batch_txt = st.empty()
                
                processed_global = 0
                
                for batch_idx, batch_df in enumerate(batches):
                    batch_num = batch_idx + 1
                    batch_results = []
                    
                    global_txt.caption(f"正在处理第 {batch_num}/{num_batches} 个任务包...")
                    
                    for i, (orig_idx, row) in enumerate(batch_df.iterrows()):
                        try:
                            # 1. 业务逻辑
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
                            
                            batch_results.append(base_res)
                            
                            # 2. 更新进度
                            processed_global += 1
                            batch_prog.progress((i + 1) / len(batch_df))
                            batch_txt.caption(f"当前包: {i+1} / {len(batch_df)} 行")
                            
                            global_prog.progress(processed_global / len(df_rem))
                            
                        except Exception as e:
                            st.warning(f"行错误: {e}")
                    
                    # 批次存档
                    if batch_results:
                        df_batch = pd.DataFrame(batch_results)
                        final_accumulated = pd.concat([final_accumulated, df_batch], ignore_index=True)
                        st.session_state.final_result_df = final_accumulated
                        st.session_state.match_stats = stats
                        st.toast(f"✅ 任务包 {batch_num} 完成！已存档。", icon="💾")

                st.success("🎉 所有任务包处理完成！")
                st.rerun()
        
        else:
            if st.button("✨ 直接生成结果", type="primary"):
                st.session_state.final_result_df = df_exact
                st.session_state.match_stats = {'exact': len(df_exact), 'high': 0, 'mid': 0, 'low': 0, 'no_match': 0}
                st.rerun()

# --- 4. 结果展示 ---
if st.session_state.final_result_df is not None:
    s = st.session_state.match_stats
    total = len(st.session_state.final_result_df)
    if total == 0: total = 1
    
    st.markdown("### 📊 匹配统计报告")
    
    exact_val = s.get('exact', 0)
    exact_pct = exact_val / total
    
    model_done = s.get('high', 0) + s.get('mid', 0) + s.get('low', 0)
    model_pct = model_done / total
    model_denom = model_done if model_done > 0 else 1
    
    high_pct = s.get('high', 0) / model_denom
    mid_pct = s.get('mid', 0) / model_denom
    low_pct = s.get('low', 0) / model_denom
    
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1: st.metric("🎯 全字匹配", f"{exact_val}", f"{exact_pct:.1%}")
    with c2: st.metric("🤖 模型总计", f"{model_done}", f"{model_pct:.1%}")
    with c3: st.metric("🔥 High", f"{s.get('high', 0)}", f"{high_pct:.1%} (of Model)")
    with c4: st.metric("⚖️ Mid", f"{s.get('mid', 0)}", f"{mid_pct:.1%} (of Model)")
    with c5: st.metric("⚠️ Low", f"{s.get('low', 0)}", f"{low_pct:.1%} (of Model)")

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
    st.download_button("📥 下载完整结果", csv, "linkmed_batch_result.csv", "text/csv", type="primary")
