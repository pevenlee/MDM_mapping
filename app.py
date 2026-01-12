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

st.set_page_config(page_title="LinkMed Matcher Pro (Fast Batch)", layout="wide", page_icon="🧬")

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
# 用于存储拆分好的任务状态
if 'prep_done' not in st.session_state:
    st.session_state.prep_done = False
if 'df_exact' not in st.session_state:
    st.session_state.df_exact = None
if 'batches' not in st.session_state:
    st.session_state.batches = []
if 'total_rem' not in st.session_state:
    st.session_state.total_rem = 0

# ================= 2. 核心工具函数 =================

def reset_app():
    """完全重置"""
    st.session_state.final_result_df = None
    st.session_state.match_stats = {}
    st.session_state.prep_done = False
    st.session_state.df_exact = None
    st.session_state.batches = []
    st.session_state.total_rem = 0
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
                model="gemini-3-flash", 
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
    分析用户数据，找出以下字段对应的列名（可能涉及中英文转化）。
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
    - 当匹配结果为low，通过药店信息中的 XX店，去主数据的地址中寻找，如果主数据中的地址包含XX，则模糊匹配上
    
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
    .task-box {background-color: #eff6ff; padding: 12px; border-radius: 6px; margin-bottom: 8px; border-left: 5px solid #2563eb; font-size:14px;}
    .prog-label {font-weight: bold; font-size: 14px; margin-bottom: 5px; display: block;}
    </style>
    <div style="font-size: 26px; font-weight: bold; color: #1E3A8A; margin-bottom: 20px;">
    🧬 LinkMed Matcher (Stable Batch)
    </div>
""", unsafe_allow_html=True)

client = get_client()

# 加载数据
df_master, prov_groups, city_groups, dist_groups, chain_groups = pd.DataFrame(), {}, {}, {}, {}
if os.path.exists(LOCAL_MASTER_FILE):
    with st.spinner(f"正在加载主数据..."):
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
        # 自动映射仅运行一次
        if 'map_config' not in st.session_state or st.session_state.get('last_file') != uploaded_file.name:
            st.session_state.prep_done = False # 重置预处理状态
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
        
        if not st.session_state.prep_done:
            if st.button("🏁 开始预处理分析", type="primary"):
                with st.spinner("正在进行极速分析与拆包..."):
                    try:
                        # 1. 安全数据清洗
                        df_safe = df_user.copy()
                        for c in [col_name, col_chain, col_prov, col_city, col_dist, col_addr]:
                            if c:
                                df_safe[c] = df_safe[c].astype(str).replace('nan', '').str.strip()
                        
                        # 2. 地理排序
                        sort_cols = []
                        if col_prov: sort_cols.append(col_prov)
                        if col_city: sort_cols.append(col_city)
                        if col_dist: sort_cols.append(col_dist)
                        if sort_cols:
                            df_safe = df_safe.sort_values(by=sort_cols).reset_index(drop=True)

                        # 3. 向量化全字匹配 (Vectorized Exact Match)
                        master_exact = df_master.drop_duplicates(subset=['标准名称']).set_index('标准名称').to_dict('index')
                        
                        def check_exact(row):
                            raw = row[col_name]
                            chain = row[col_chain] if col_chain else ""
                            search = raw
                            if chain and chain not in raw: search = f"{chain} {raw}"
                            
                            if search in master_exact:
                                m = master_exact[search]
                                return pd.Series([
                                    True, m.get('esid'), search, m.get('机构类型'), "High", "全字匹配", "精确命中"
                                ])
                            return pd.Series([False, None, None, None, None, None, None])

                        # 批量应用逻辑
                        match_results = df_safe.apply(check_exact, axis=1)
                        match_results.columns = ['is_match', '匹配ESID', '匹配标准名', '机构类型', '置信度', '匹配方式', '理由']
                        
                        # 合并结果
                        df_combined = pd.concat([df_safe, match_results], axis=1)
                        
                        # 拆分结果
                        df_exact = df_combined[df_combined['is_match'] == True].drop(columns=['is_match'])
                        df_rem = df_combined[df_combined['is_match'] == False].drop(columns=['is_match', '匹配ESID', '匹配标准名', '机构类型', '置信度', '匹配方式', '理由'])
                        
                        # 存入 Session
                        st.session_state.df_exact = df_exact
                        st.session_state.total_rem = len(df_rem)
                        
                        # 4. 拆分批次
                        batches = []
                        if len(df_rem) > 0:
                            BATCH_SIZE = 1000 
                            num_batches = math.ceil(len(df_rem) / BATCH_SIZE)
                            for i in range(num_batches):
                                batches.append(df_rem.iloc[i*BATCH_SIZE : (i+1)*BATCH_SIZE])
                        
                        st.session_state.batches = batches
                        st.session_state.prep_done = True
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"预处理发生错误: {e}")
                        st.stop()
        
        # --- 渲染任务列表 ---
        if st.session_state.prep_done:
            count_exact = len(st.session_state.df_exact)
            count_rem = st.session_state.total_rem
            batches = st.session_state.batches
            
            st.info(f"✅ 预处理完成：自动命中 {count_exact} 行。剩余 {count_rem} 行待模型匹配。")
            
            if count_rem > 0:
                st.markdown(f"**已拆分为 {len(batches)} 个任务包（每包约 1000 条），防止内存溢出。**")
                
                # 可折叠的任务预览
                with st.expander(f"👁️ 查看 {len(batches)} 个任务包详情", expanded=False):
                    for i, b in enumerate(batches):
                        tag = "混合区域"
                        if len(b) > 0:
                            r = b.iloc[0]
                            p = r[col_prov] if col_prov else ""
                            c = r[col_city] if col_city else ""
                            if p or c: tag = f"{p} {c}"
                        st.markdown(f"<div class='task-box'>📦 <b>任务包 {i+1}</b>: {len(b)} 行 <small>({tag})</small></div>", unsafe_allow_html=True)
                
                # 启动按钮
                if st.button(f"🚀 启动任务队列 ({count_rem} 行)", type="primary"):
                    
                    final_accumulated = st.session_state.df_exact.copy()
                    stats = {'exact': count_exact, 'high': 0, 'mid': 0, 'low': 0, 'no_match': 0}
                    
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
                    
                    # 🚀 执行循环
                    for batch_idx, batch_df in enumerate(batches):
                        batch_num = batch_idx + 1
                        batch_results = []
                        
                        global_txt.caption(f"正在处理包 {batch_num}/{len(batches)} ...")
                        
                        for i, (orig_idx, row) in enumerate(batch_df.iterrows()):
                            try:
                                # 数据准备
                                raw_name = str(row[col_name])
                                chain_name = str(row[col_chain]) if col_chain else ""
                                search_name = raw_name
                                if chain_name and chain_name not in raw_name: search_name = f"{chain_name} {raw_name}"
                                
                                row_with_meta = row.copy()
                                if col_addr: row_with_meta['地址列_raw'] = str(row[col_addr])

                                # 检索
                                indices, scope_desc = get_candidates_hierarchical(
                                    search_name, chain_name, df_master, 
                                    prov_groups, city_groups, dist_groups, chain_groups, 
                                    row, mapping
                                )
                                
                                base_res = row.to_dict()
                                
                                # 结果判断
                                if not indices:
                                    base_res.update({"匹配ESID": None, "匹配标准名": None, "机构类型": None, "置信度": "Low", "匹配方式": "无结果", "理由": f"范围[{scope_desc}]内无候选"})
                                    stats['no_match'] += 1
                                else:
                                    candidates = df_master.loc[indices].copy()
                                    if candidates.empty:
                                        base_res.update({"匹配ESID": None, "匹配标准名": None, "机构类型": None, "置信度": "Low", "匹配方式": "无结果", "理由": "索引异常"})
                                        stats['no_match'] += 1
                                    else:
                                        # AI 匹配
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
                                        
                                        time.sleep(1.5) # 冷却
                                
                                batch_results.append(base_res)
                                
                                # 更新进度
                                processed_global += 1
                                batch_prog.progress((i + 1) / len(batch_df))
                                batch_txt.caption(f"进度: {i+1}/{len(batch_df)}")
                                global_prog.progress(processed_global / count_rem)
                                
                            except Exception as e:
                                pass
                        
                        # --- 批次存档 ---
                        if batch_results:
                            df_batch = pd.DataFrame(batch_results)
                            final_accumulated = pd.concat([final_accumulated, df_batch], ignore_index=True)
                            st.session_state.final_result_df = final_accumulated
                            st.session_state.match_stats = stats
                            st.toast(f"✅ 任务包 {batch_num} 完成并存档", icon="💾")
                            del df_batch
                            gc.collect()

                    st.success("🎉 所有任务处理完成！")
                    st.rerun()
            
            else:
                # 只有全字匹配
                if st.button("✨ 直接生成结果", type="primary"):
                    st.session_state.final_result_df = st.session_state.df_exact
                    st.session_state.match_stats = {'exact': count_exact, 'high': 0, 'mid': 0, 'low': 0, 'no_match': 0}
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




