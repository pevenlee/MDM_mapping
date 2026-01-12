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

st.set_page_config(page_title="LinkMed Matcher Pro (Batch)", layout="wide", page_icon="🧬")

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
    st.session_state.batch_progress = [] # 存储已完成的批次信息

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
    """加载并建立严格的地理分层索引"""
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
                if intersection:
                    target_indices = intersection
                    scope_desc = f"精准定位: {u_city}{u_dist}"
                else:
                    target_indices = dist_indices
                    scope_desc = f"区域定位: {u_dist}"
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
    .sub-text {font-size: 14px; color: #6b7280;}
    .task-box {background-color: #f3f4f6; padding: 10px; border-radius: 5px; margin-bottom: 5px; border-left: 4px solid #3b82f6;}
    </style>
    <div style="font-size: 26px; font-weight: bold; color: #1E3A8A; margin-bottom: 20px;">
    🧬 LinkMed Matcher (Batch Processing)
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
        st.markdown("### ⚡ 3. 预处理与分包")
        
        # 🌟 第一步：分组重排 (Clustering)
        # 将数据按省市区排序，保证同一个地区的店在一起，利于分包
        sort_cols = []
        if col_prov: sort_cols.append(col_prov)
        if col_city: sort_cols.append(col_city)
        if col_dist: sort_cols.append(col_dist)
        
        if sort_cols:
            df_user_sorted = df_user.sort_values(by=sort_cols).reset_index(drop=True)
            st.caption(f"✅ 已按地理位置重排数据，优化匹配效率。")
        else:
            df_user_sorted = df_user

        # 🌟 第二步：全字匹配过滤
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
        
        # 🌟 第三步：智能拆包 (Batch Splitting)
        # 如果剩余数据 > 2000，则拆分
        BATCH_SIZE = 2000
        num_batches = 1
        batches = []
        
        if len(df_rem) > 0:
            num_batches = math.ceil(len(df_rem) / BATCH_SIZE)
            for i in range(num_batches):
                start_i = i * BATCH_SIZE
                end_i = min((i + 1) * BATCH_SIZE, len(df_rem))
                batch_df = df_rem.iloc[start_i:end_i]
                batches.append(batch_df)

        st.info(f"预处理报告: 自动命中 {len(df_exact)} 行。剩余 {len(df_rem)} 行待模型匹配。")
        
        if len(df_rem) > 0:
            st.warning(f"由于数据量较大，已自动拆分为 **{num_batches}** 个任务包，将依次执行并自动合并结果。")
            with st.expander("查看任务队列", expanded=True):
                for i, b in enumerate(batches):
                    # 尝试读取该批次的第一个地理位置作为标识
                    loc_tag = "未知区域"
                    if len(b) > 0:
                        first_row = b.iloc[0]
                        p = str(first_row[col_prov]) if col_prov and pd.notna(first_row[col_prov]) else ""
                        c = str(first_row[col_city]) if col_city and pd.notna(first_row[col_city]) else ""
                        loc_tag = f"{p} {c}".strip() or "混合区域"
                    st.markdown(f"<div class='task-box'>📦 <b>任务包 {i+1}</b>: {len(b)} 行 (起始位置: {loc_tag})</div>", unsafe_allow_html=True)

            if st.button(f"🚀 启动任务队列 ({len(df_rem)} 行)", type="primary"):
                
                # 初始化结果容器 (包含已全字匹配的)
                if df_exact.empty:
                    final_accumulated = pd.DataFrame()
                else:
                    final_accumulated = df_exact.copy()
                
                stats = {'exact': len(df_exact), 'high': 0, 'mid': 0, 'low': 0, 'no_match': 0}
                
                # 进度条 (针对所有模型任务)
                total_rem_rows = len(df_rem)
                global_prog = st.progress(0)
                status_text = st.empty()
                
                processed_count = 0
                
                # --- 循环执行批次 ---
                for batch_idx, batch_df in enumerate(batches):
                    
                    status_text.markdown(f"### 🔄 正在处理任务包 {batch_idx + 1} / {num_batches} ...")
                    batch_results = []
                    
                    for i, (orig_idx, row) in enumerate(batch_df.iterrows()):
                        try:
                            # 准备数据
                            raw_name = str(row[col_name]).strip()
                            chain_name = str(row[col_chain]).strip() if col_chain and pd.notna(row[col_chain]) else ""
                            search_name = raw_name
                            if chain_name and chain_name not in raw_name: search_name = f"{chain_name} {raw_name}"
                            
                            row_with_meta = row.copy()
                            if col_addr: row_with_meta['地址列_raw'] = str(row[col_addr])

                            # 调用分层检索
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
                                    # 调用 AI
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
                            
                            processed_count += 1
                            progress_pct = processed_count / total_rem_rows
                            global_prog.progress(progress_pct)
                            
                        except Exception as e:
                            st.warning(f"行错误: {e}")
                    
                    # --- 🌟 批次存档点 ---
                    # 每跑完一个包，立即更新结果
                    if batch_results:
                        df_batch = pd.DataFrame(batch_results)
                        final_accumulated = pd.concat([final_accumulated, df_batch], ignore_index=True)
                        
                        # 存入 Session State (防止浏览器崩溃后丢失全部)
                        st.session_state.final_result_df = final_accumulated
                        st.session_state.match_stats = stats
                        
                        # 显示临时下载 (可选，给极度谨慎的用户)
                        # 注意：st.download_button 在循环中不能直接点击，这里主要起展示作用，或者用 st.empty 更新
                        st.toast(f"✅ 任务包 {batch_idx + 1} 完成！已自动保存进度。", icon="💾")

                # 循环结束，最终跳转
                st.success("🎉 所有任务包处理完成！")
                st.rerun()
        
        else:
            # 只有全字匹配的情况
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
    st.download_button("📥 下载完整结果 (含所有批次)", csv, "linkmed_batch_result.csv", "text/csv", type="primary")
