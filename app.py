import streamlit as st
import pandas as pd
import sqlite3
import json
import os
import io
from datetime import datetime
from pathlib import Path

# =============================================================================
# 設定
# =============================================================================
DB_PATH = "database.db"
METADATA_PATH = "metadata.json"

# Metadata 要追蹤的欄位
METADATA_COLUMNS = {
    "EE_BOM": [
        "Project_Name",
        "PARENT_DPN",
        "COMMODITY_CODE",
        "SUB_COMMODITY",
        "DPN",
        "ODM_PN",
        "MANUFACTURER",
        "MPN",
        "EM_DM",
        "Quarter",
    ],
    "Cost_Adder_Logistic": [
        "Project_Name",
        "Parent_DPN",
        "Sub_Cost_Category",
        "Region",
        "Quarter",
    ],
}

# Quarter 對照表
QUARTER_TABLE = [
    ("FY24Q1", "2023-02-04", "2023-05-05"),
    ("FY24Q2", "2023-05-06", "2023-08-04"),
    ("FY24Q3", "2023-08-05", "2023-11-03"),
    ("FY24Q4", "2023-11-04", "2024-02-02"),
    ("FY25Q1", "2024-02-03", "2024-05-03"),
    ("FY25Q2", "2024-05-04", "2024-08-02"),
    ("FY25Q3", "2024-08-03", "2024-11-01"),
    ("FY25Q4", "2024-11-02", "2025-01-31"),
    ("FY26Q1", "2025-02-01", "2025-05-02"),
    ("FY26Q2", "2025-05-03", "2025-08-01"),
    ("FY26Q3", "2025-08-02", "2025-10-31"),
    ("FY26Q4", "2025-11-01", "2026-01-30"),
    ("FY27Q1", "2026-01-31", "2026-05-01"),
    ("FY27Q2", "2026-05-02", "2026-07-31"),
    ("FY27Q3", "2026-08-01", "2026-10-30"),
    ("FY27Q4", "2026-10-31", "2027-01-29"),
    ("FY28Q1", "2027-01-30", "2027-04-30"),
    ("FY28Q2", "2027-05-01", "2027-07-30"),
    ("FY28Q3", "2027-07-31", "2027-10-29"),
    ("FY28Q4", "2027-10-30", "2028-01-28"),
    ("FY29Q1", "2028-01-29", "2028-04-28"),
    ("FY29Q2", "2028-04-29", "2028-07-28"),
    ("FY29Q3", "2028-07-29", "2028-10-27"),
    ("FY29Q4", "2028-10-28", "2029-02-02"),
    ("FY30Q1", "2029-02-03", "2029-05-04"),
    ("FY30Q2", "2029-05-05", "2029-08-03"),
    ("FY30Q3", "2029-08-04", "2029-11-02"),
    ("FY30Q4", "2029-11-03", "2030-02-01"),
    ("FY31Q1", "2030-02-02", "2030-05-03"),
    ("FY31Q2", "2030-05-04", "2030-08-02"),
    ("FY31Q3", "2030-08-03", "2030-11-01"),
    ("FY31Q4", "2030-11-02", "2031-01-31"),
    ("FY32Q1", "2031-02-01", "2031-05-02"),
    ("FY32Q2", "2031-05-03", "2031-08-01"),
    ("FY32Q3", "2031-08-02", "2031-10-31"),
    ("FY32Q4", "2031-11-01", "2032-01-30"),
    ("FY33Q1", "2032-01-31", "2032-04-30"),
    ("FY33Q2", "2032-05-01", "2032-07-30"),
    ("FY33Q3", "2032-07-31", "2032-10-29"),
    ("FY33Q4", "2032-10-30", "2033-01-28"),
    ("FY34Q1", "2033-01-29", "2033-04-29"),
    ("FY34Q2", "2033-04-30", "2033-07-29"),
    ("FY34Q3", "2033-07-30", "2033-10-28"),
    ("FY34Q4", "2033-10-29", "2034-02-03"),
    ("FY35Q1", "2034-02-04", "2034-05-05"),
    ("FY35Q2", "2034-05-06", "2034-08-04"),
    ("FY35Q3", "2034-08-05", "2034-11-03"),
    ("FY35Q4", "2034-11-04", "2035-02-02"),
]

# 建立 Quarter 列表（用於下拉選單）
QUARTER_LIST = [q[0] for q in QUARTER_TABLE]


# =============================================================================
# Quarter 工具函數
# =============================================================================
def date_to_quarter(date_value) -> str:
    """將日期轉換為 Quarter"""
    if pd.isna(date_value):
        return None
    
    # 轉換為 datetime
    if isinstance(date_value, str):
        try:
            date_value = pd.to_datetime(date_value)
        except:
            return None
    elif not isinstance(date_value, (datetime, pd.Timestamp)):
        try:
            date_value = pd.to_datetime(date_value)
        except:
            return None
    
    # 查找對應的 Quarter
    for quarter, start_str, end_str in QUARTER_TABLE:
        start_date = pd.to_datetime(start_str)
        end_date = pd.to_datetime(end_str)
        if start_date <= date_value <= end_date:
            return quarter
    
    return None


def get_next_quarter(quarter: str) -> str:
    """取得下一個 Quarter"""
    if quarter not in QUARTER_LIST:
        return None
    
    idx = QUARTER_LIST.index(quarter)
    if idx + 1 < len(QUARTER_LIST):
        return QUARTER_LIST[idx + 1]
    return None


def get_quarter_distance(q1: str, q2: str) -> int:
    """
    計算兩個 Quarter 之間的距離（季數）
    q1: 起始 Quarter
    q2: 結束 Quarter
    回傳：q2 - q1 的季數
    """
    if q1 not in QUARTER_LIST or q2 not in QUARTER_LIST:
        return None
    
    idx1 = QUARTER_LIST.index(q1)
    idx2 = QUARTER_LIST.index(q2)
    return idx2 - idx1


def get_current_quarter() -> str:
    """取得當前日期對應的 Quarter"""
    return date_to_quarter(datetime.now())


# =============================================================================
# 資料庫操作
# =============================================================================
def get_db_connection():
    """取得資料庫連線"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_database():
    """初始化資料庫"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 建立 Plant_Generation table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Plant_Generation (
            Project_Name TEXT,
            Parent_DPN TEXT,
            Plant TEXT,
            Generation TEXT,
            PRIMARY KEY (Project_Name, Parent_DPN)
        )
    """)
    
    # 建立 Project_MVA_Info table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Project_MVA_Info (
            Project_Name TEXT PRIMARY KEY,
            Initial_MVA REAL,
            Initial_Quarter TEXT,
            Adder REAL
        )
    """)
    
    conn.commit()
    conn.close()


def table_exists(table_name: str) -> bool:
    """檢查 table 是否存在"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,)
    )
    result = cursor.fetchone()
    conn.close()
    return result is not None


def insert_data(table_name: str, df: pd.DataFrame) -> int:
    """
    插入資料到 table，跳過重複資料
    回傳實際新增的筆數
    """
    if df.empty:
        return 0
    
    conn = get_db_connection()
    
    # 加入 created_at 欄位
    df_to_insert = df.copy()
    df_to_insert["created_at"] = datetime.now().isoformat()
    
    # 如果 table 不存在，先建立
    if not table_exists(table_name):
        df_to_insert.to_sql(table_name, conn, if_exists="replace", index=False)
        conn.close()
        return len(df_to_insert)
    
    # 取得現有資料（不含 created_at）
    existing_df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    
    # 比較用的欄位（排除 created_at）
    compare_cols = [col for col in df.columns if col != "created_at"]
    
    # 找出不重複的資料
    if not existing_df.empty:
        # 確保只比較兩邊都有的欄位
        common_cols = [col for col in compare_cols if col in existing_df.columns]
        
        # 將所有欄位轉換為字串以避免資料類型不一致的問題
        df_compare = df[common_cols].astype(str)
        existing_compare = existing_df[common_cols].astype(str).drop_duplicates()
        
        # 將 df 與 existing 合併，找出新資料
        merged = df_compare.merge(
            existing_compare,
            how="left",
            indicator=True
        )
        new_mask = merged["_merge"] == "left_only"
        new_df = df_to_insert[new_mask.values]
    else:
        new_df = df_to_insert
    
    # 插入新資料
    if not new_df.empty:
        new_df.to_sql(table_name, conn, if_exists="append", index=False)
    
    conn.close()
    return len(new_df)


def query_data(table_name: str, filters: dict) -> pd.DataFrame:
    """
    根據篩選條件查詢資料
    filters: {column_name: [value1, value2, ...], ...}
    """
    if not table_exists(table_name):
        return pd.DataFrame()
    
    conn = get_db_connection()
    
    # 建立 SQL 查詢
    query = f"SELECT * FROM {table_name}"
    conditions = []
    params = []
    
    for col, values in filters.items():
        if values:  # 只處理有選擇值的欄位
            placeholders = ", ".join(["?" for _ in values])
            conditions.append(f"{col} IN ({placeholders})")
            params.extend(values)
    
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    
    df = pd.read_sql(query, conn, params=params)
    conn.close()
    
    # 移除 created_at 欄位（不需要在報表中顯示）
    if "created_at" in df.columns:
        df = df.drop(columns=["created_at"])
    
    return df


def get_all_data(table_name: str) -> pd.DataFrame:
    """取得 table 中所有資料"""
    if not table_exists(table_name):
        return pd.DataFrame()
    
    conn = get_db_connection()
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    conn.close()
    return df


# =============================================================================
# Plant_Generation 操作
# =============================================================================
def upsert_plant_generation(df: pd.DataFrame) -> int:
    """插入或更新 Plant_Generation 資料"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    count = 0
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT OR REPLACE INTO Plant_Generation 
            (Project_Name, Parent_DPN, Plant, Generation)
            VALUES (?, ?, ?, ?)
        """, (row["Project_Name"], row["Parent_DPN"], row["Plant"], row["Generation"]))
        count += 1
    
    conn.commit()
    conn.close()
    return count


def get_plant_generation() -> pd.DataFrame:
    """取得所有 Plant_Generation 資料"""
    conn = get_db_connection()
    try:
        df = pd.read_sql("SELECT * FROM Plant_Generation", conn)
    except:
        df = pd.DataFrame()
    conn.close()
    return df


# =============================================================================
# Project_MVA_Info 操作
# =============================================================================
def upsert_project_mva_info(project_name: str, initial_mva: float, 
                            initial_quarter: str, adder: float):
    """插入或更新 Project_MVA_Info"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT OR REPLACE INTO Project_MVA_Info 
        (Project_Name, Initial_MVA, Initial_Quarter, Adder)
        VALUES (?, ?, ?, ?)
    """, (project_name, initial_mva, initial_quarter, adder))
    
    conn.commit()
    conn.close()


def get_project_mva_info(project_name: str = None) -> pd.DataFrame:
    """取得 Project_MVA_Info 資料"""
    conn = get_db_connection()
    try:
        if project_name:
            df = pd.read_sql(
                "SELECT * FROM Project_MVA_Info WHERE Project_Name = ?",
                conn, params=[project_name]
            )
        else:
            df = pd.read_sql("SELECT * FROM Project_MVA_Info", conn)
    except:
        df = pd.DataFrame()
    conn.close()
    return df


def get_all_project_names() -> list:
    """取得所有不重複的 Project_Name"""
    projects = set()
    
    # 從 EE_BOM 取得
    if table_exists("EE_BOM"):
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT Project_Name FROM EE_BOM")
        for row in cursor.fetchall():
            if row[0]:
                projects.add(row[0])
        conn.close()
    
    # 從 Project_MVA_Info 取得
    if table_exists("Project_MVA_Info"):
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT Project_Name FROM Project_MVA_Info")
        for row in cursor.fetchall():
            if row[0]:
                projects.add(row[0])
        conn.close()
    
    return sorted(list(projects))


# =============================================================================
# Metadata 操作
# =============================================================================
def load_metadata() -> dict:
    """載入 metadata.json"""
    if os.path.exists(METADATA_PATH):
        with open(METADATA_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"EE_BOM": {}, "Cost_Adder_Logistic": {}}


def save_metadata(metadata: dict):
    """儲存 metadata.json"""
    with open(METADATA_PATH, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)


def refresh_metadata():
    """重新掃描資料庫，更新 metadata"""
    metadata = {"EE_BOM": {}, "Cost_Adder_Logistic": {}}
    
    conn = get_db_connection()
    
    for table_name, columns in METADATA_COLUMNS.items():
        if not table_exists(table_name):
            continue
        
        metadata[table_name] = {}
        
        for col in columns:
            # 檢查欄位是否存在
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            table_columns = [row[1] for row in cursor.fetchall()]
            
            if col not in table_columns:
                continue
            
            # 取得 unique values，按 created_at 由新到舊排序
            query = f"""
                SELECT DISTINCT {col}, MAX(created_at) as last_seen
                FROM {table_name}
                WHERE {col} IS NOT NULL AND {col} != ''
                GROUP BY {col}
                ORDER BY last_seen DESC
            """
            cursor.execute(query)
            results = cursor.fetchall()
            
            # 只保留值，不保留 timestamp
            unique_values = [str(row[0]) for row in results]
            metadata[table_name][col] = unique_values
    
    conn.close()
    save_metadata(metadata)
    return metadata


# =============================================================================
# 檔名解析
# =============================================================================
def parse_project_name(filename: str) -> str:
    """
    從檔名解析 Project_Name
    規則：以底線分隔，取第 3 個欄位（index 2）
    """
    name_without_ext = Path(filename).stem
    parts = name_without_ext.split("_")
    
    if len(parts) >= 3:
        return parts[2]
    else:
        return name_without_ext


# =============================================================================
# 預估 EM/MVA 計算
# =============================================================================
def is_empty_value(value) -> bool:
    """判斷值是否為空（NULL、空字串、純空白）"""
    if pd.isna(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def calculate_em_mva(cur_quarter: str) -> pd.DataFrame:
    """計算 EM/MVA 預估報表"""
    next_quarter = get_next_quarter(cur_quarter)
    
    if not next_quarter:
        return pd.DataFrame()
    
    # 取得資料
    ee_bom_df = query_data("EE_BOM", {"Quarter": [cur_quarter]})
    cost_adder_df = get_all_data("Cost_Adder_Logistic")
    plant_gen_df = get_plant_generation()
    mva_info_df = get_project_mva_info()
    
    if ee_bom_df.empty:
        return pd.DataFrame()
    
    # 取得所有 PARENT_DPN
    parent_dpns = ee_bom_df["PARENT_DPN"].unique()
    
    results = []
    
    for parent_dpn in parent_dpns:
        # 篩選該 PARENT_DPN 的資料
        dpn_ee_bom = ee_bom_df[ee_bom_df["PARENT_DPN"] == parent_dpn]
        
        # 取得 Project_Name
        project_name = dpn_ee_bom["Project_Name"].iloc[0] if not dpn_ee_bom.empty else None
        
        # 取得 Plant 和 Generation
        plant = None
        generation = None
        if not plant_gen_df.empty and project_name:
            pg_match = plant_gen_df[
                (plant_gen_df["Project_Name"] == project_name) & 
                (plant_gen_df["Parent_DPN"] == parent_dpn)
            ]
            if not pg_match.empty:
                plant = pg_match["Plant"].iloc[0]
                generation = pg_match["Generation"].iloc[0]
        
        # 取得 MVA Info
        initial_mva = None
        initial_quarter = None
        adder = None
        if not mva_info_df.empty and project_name:
            mva_match = mva_info_df[mva_info_df["Project_Name"] == project_name]
            if not mva_match.empty:
                initial_mva = mva_match["Initial_MVA"].iloc[0]
                initial_quarter = mva_match["Initial_Quarter"].iloc[0]
                adder = mva_match["Adder"].iloc[0]
        
        # 計算 EM cost incl. QoQ & concession (cur_quarter)
        cur_em_cost_total = dpn_ee_bom["EXT_COST"].sum() if "EXT_COST" in dpn_ee_bom.columns else 0
        
        # 計算 EM (w/ QoQ part) - BOM_COMMENT 為空的
        if "BOM_COMMENT" in dpn_ee_bom.columns:
            empty_comment_mask = dpn_ee_bom["BOM_COMMENT"].apply(is_empty_value)
            cur_em_w_qoq = dpn_ee_bom.loc[empty_comment_mask, "EXT_COST"].sum() if "EXT_COST" in dpn_ee_bom.columns else 0
            # 計算 EM (w/o QoQ part) - BOM_COMMENT 不為空的
            next_em_wo_qoq = dpn_ee_bom.loc[~empty_comment_mask, "EXT_COST"].sum() if "EXT_COST" in dpn_ee_bom.columns else 0
        else:
            cur_em_w_qoq = cur_em_cost_total
            next_em_wo_qoq = 0
        
        # 計算衰減率
        decay_rate = 0
        if initial_quarter:
            quarter_distance = get_quarter_distance(initial_quarter, cur_quarter)
            if quarter_distance is not None and quarter_distance < 8:
                decay_rate = 0.02
        
        # 計算 next_quarter EM (w/ QoQ part)
        next_em_w_qoq = cur_em_w_qoq * (1 - decay_rate)
        
        # 計算 next_quarter EM cost incl. QoQ & concession
        next_em_cost_total = next_em_w_qoq + next_em_wo_qoq
        
        # 取得 cur_quarter MVA incl. QoQ
        cur_mva = None
        if not cost_adder_df.empty:
            mva_match = cost_adder_df[
                (cost_adder_df["Parent_DPN"] == parent_dpn) & 
                (cost_adder_df["Sub_Cost_Category"] == "MVA")
            ]
            if not mva_match.empty:
                cur_mva = mva_match["Unit_Cost"].iloc[0]
        
        # 計算 next_quarter MVA incl. QoQ
        next_mva = None
        if initial_mva is not None and initial_quarter and adder is not None:
            delta_q = get_quarter_distance(initial_quarter, cur_quarter)
            if delta_q is not None:
                delta_q += 1  # (cur_quarter - Initial_Quarter) + 1
                if delta_q > 8:
                    next_mva = cur_mva
                else:
                    next_mva = round(initial_mva * (0.98 ** delta_q)) + adder
        
        # 組合結果
        results.append({
            "Plant": plant,
            "Generation": generation,
            "Project_Name": project_name,
            "PARENT_DPN": parent_dpn,
            f"{cur_quarter} EM cost incl. QoQ & concession": cur_em_cost_total,
            f"{next_quarter} EM cost incl. QoQ & concession": next_em_cost_total,
            f"{cur_quarter} EM (w/ QoQ part)": cur_em_w_qoq,
            f"{next_quarter} EM (w/ QoQ part)": next_em_w_qoq,
            f"{next_quarter} EM (w/o QoQ part)": next_em_wo_qoq,
            f"{cur_quarter} MVA incl. QoQ": cur_mva,
            f"{next_quarter} MVA incl. QoQ": next_mva,
        })
    
    return pd.DataFrame(results)


# =============================================================================
# Streamlit UI
# =============================================================================
def main():
    st.set_page_config(
        page_title="BOM 資料管理系統",
        page_icon="📊",
        layout="wide"
    )
    
    st.title("📊 BOM 資料管理系統")
    
    # 初始化資料庫
    init_database()
    
    # 側邊欄選單
    page = st.sidebar.radio(
        "功能選擇",
        ["維護 Project/Parent_DPN", "預估 EM/MVA", "上傳資料", "產生報表"],
        index=0
    )
    
    if page == "維護 Project/Parent_DPN":
        maintenance_page()
    elif page == "預估 EM/MVA":
        estimate_page()
    elif page == "上傳資料":
        upload_page()
    else:
        report_page()


def maintenance_page():
    """維護 Project/Parent_DPN 頁面"""
    st.header("🔧 維護 Project/Parent_DPN")
    
    tab1, tab2 = st.tabs(["Plant and Generation", "Project MVA Info"])
    
    # =========================================================================
    # Tab 1: Plant and Generation
    # =========================================================================
    with tab1:
        st.subheader("📍 Plant and Generation")
        
        st.info("""
        **使用說明：**
        上傳包含以下欄位的 Excel 檔案：`Project_Name`, `Parent_DPN`, `Plant`, `Generation`
        - 若 `Project_Name` + `Parent_DPN` 已存在，將會覆蓋更新
        """)
        
        uploaded_file = st.file_uploader(
            "選擇 Excel 檔案",
            type=["xlsx"],
            key="plant_gen_uploader"
        )
        
        if uploaded_file:
            try:
                df = pd.read_excel(uploaded_file)
                
                # 檢查必要欄位
                required_cols = ["Project_Name", "Parent_DPN", "Plant", "Generation"]
                missing_cols = [c for c in required_cols if c not in df.columns]
                
                if missing_cols:
                    st.error(f"❌ 缺少欄位：{', '.join(missing_cols)}")
                else:
                    st.write("**資料預覽：**")
                    st.dataframe(df.head(10), use_container_width=True)
                    
                    if st.button("✅ 確認上傳", key="upload_plant_gen"):
                        count = upsert_plant_generation(df[required_cols])
                        st.success(f"✅ 成功更新 {count} 筆資料！")
            except Exception as e:
                st.error(f"❌ 讀取檔案錯誤：{str(e)}")
        
        # 顯示現有資料
        st.write("---")
        st.subheader("📋 現有資料")
        existing_df = get_plant_generation()
        if not existing_df.empty:
            st.dataframe(existing_df, use_container_width=True)
        else:
            st.info("尚無資料")
    
    # =========================================================================
    # Tab 2: Project MVA Info
    # =========================================================================
    with tab2:
        st.subheader("💰 Project MVA Info")
        
        # 取得所有 Project_Name
        project_names = get_all_project_names()
        
        col1, col2 = st.columns([3, 1])
        with col1:
            if project_names:
                selected_project = st.selectbox(
                    "選擇 Project",
                    options=[""] + project_names,
                    help="選擇要編輯的 Project，或在下方輸入新的 Project Name"
                )
            else:
                selected_project = ""
                st.info("尚無 Project 資料，請先上傳 BOM 檔案或直接輸入新 Project")
        
        with col2:
            new_project = st.text_input("或輸入新 Project Name")
        
        # 決定使用哪個 Project Name
        project_to_edit = new_project if new_project else selected_project
        
        if project_to_edit:
            # 載入現有資料（如果有的話）
            existing_info = get_project_mva_info(project_to_edit)
            
            default_mva = existing_info["Initial_MVA"].iloc[0] if not existing_info.empty else 0.0
            default_quarter = existing_info["Initial_Quarter"].iloc[0] if not existing_info.empty else QUARTER_LIST[0]
            default_adder = existing_info["Adder"].iloc[0] if not existing_info.empty else 0.0
            
            # 找到預設 Quarter 的 index
            default_quarter_idx = 0
            if default_quarter in QUARTER_LIST:
                default_quarter_idx = QUARTER_LIST.index(default_quarter)
            
            st.write(f"**編輯 Project: `{project_to_edit}`**")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                initial_mva = st.number_input(
                    "Initial MVA",
                    value=float(default_mva),
                    format="%.2f"
                )
            
            with col2:
                initial_quarter = st.selectbox(
                    "Initial Quarter",
                    options=QUARTER_LIST,
                    index=default_quarter_idx
                )
            
            with col3:
                adder = st.number_input(
                    "Adder",
                    value=float(default_adder),
                    format="%.2f"
                )
            
            if st.button("💾 儲存", key="save_mva_info"):
                upsert_project_mva_info(project_to_edit, initial_mva, initial_quarter, adder)
                st.success(f"✅ 已儲存 `{project_to_edit}` 的 MVA Info！")
                st.rerun()
        
        # 顯示現有資料
        st.write("---")
        st.subheader("📋 現有 Project MVA Info")
        all_mva_info = get_project_mva_info()
        if not all_mva_info.empty:
            st.dataframe(all_mva_info, use_container_width=True)
        else:
            st.info("尚無資料")


def estimate_page():
    """預估 EM/MVA 頁面"""
    st.header("📈 預估 EM/MVA")
    
    # 選擇 Quarter
    col1, col2 = st.columns(2)
    
    with col1:
        # 取得預設 Quarter index
        current_q = get_current_quarter()
        default_idx = QUARTER_LIST.index(current_q) if current_q in QUARTER_LIST else 0
        
        cur_quarter = st.selectbox(
            "選擇當前 Quarter",
            options=QUARTER_LIST,
            index=default_idx
        )
    
    with col2:
        next_quarter = get_next_quarter(cur_quarter)
        st.text_input("下一個 Quarter", value=next_quarter or "N/A", disabled=True)
    
    if not next_quarter:
        st.warning("⚠️ 無法計算下一個 Quarter（已到達最後一季）")
        return
    
    # 計算按鈕
    if st.button("🔄 計算預估", type="primary", use_container_width=True):
        with st.spinner("正在計算..."):
            result_df = calculate_em_mva(cur_quarter)
        
        if result_df.empty:
            st.warning(f"⚠️ 在 {cur_quarter} 沒有找到任何 EE_BOM 資料")
        else:
            st.session_state["estimate_result"] = result_df
            st.session_state["estimate_cur_quarter"] = cur_quarter
            st.session_state["estimate_next_quarter"] = next_quarter
    
    # 顯示結果
    if "estimate_result" in st.session_state:
        result_df = st.session_state["estimate_result"]
        cur_q = st.session_state["estimate_cur_quarter"]
        next_q = st.session_state["estimate_next_quarter"]
        
        st.write("---")
        st.subheader("📊 計算結果")
        st.write(f"共 {len(result_df)} 筆資料")
        st.dataframe(result_df, use_container_width=True)
        
        # 下載按鈕
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            result_df.to_excel(writer, sheet_name="EM_MVA_Estimate", index=False)
        excel_data = output.getvalue()
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"EM_MVA_Estimate_{cur_q}_{timestamp}.xlsx"
        
        st.download_button(
            label="⬇️ 下載 Excel 報表",
            data=excel_data,
            file_name=filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            type="primary"
        )


def upload_page():
    """上傳資料頁面"""
    st.header("📤 上傳資料")
    
    st.info("""
    **使用說明：**
    1. 上傳 Excel 檔案（.xlsx）
    2. 檔名格式範例：`(Dell) SEBOM_Foxconn_Boss S2_PROD_Quote_20250411.xlsx`
    3. 系統會自動讀取 `EE_BOM` 和 `Cost_Adder_Logistic` 兩個 Sheet
    4. `Effective_Start_Date` 會自動轉換為 `Quarter`
    """)
    
    uploaded_file = st.file_uploader(
        "選擇 Excel 檔案",
        type=["xlsx"],
        help="請上傳包含 EE_BOM 和 Cost_Adder_Logistic 工作表的 Excel 檔案"
    )
    
    if uploaded_file is not None:
        # 解析 Project_Name
        project_name = parse_project_name(uploaded_file.name)
        
        st.write("---")
        st.subheader("📋 檔案資訊")
        col1, col2 = st.columns(2)
        with col1:
            st.write(f"**檔案名稱：** {uploaded_file.name}")
        with col2:
            st.write(f"**解析出的 Project_Name：** `{project_name}`")
        
        # 讀取 Excel
        try:
            excel_file = pd.ExcelFile(uploaded_file)
            sheet_names = excel_file.sheet_names
            
            # 檢查必要的 Sheet
            required_sheets = ["EE_BOM", "Cost_Adder_Logistic"]
            missing_sheets = [s for s in required_sheets if s not in sheet_names]
            
            if missing_sheets:
                st.error(f"❌ 缺少以下工作表：{', '.join(missing_sheets)}")
                st.write(f"檔案中的工作表：{', '.join(sheet_names)}")
                return
            
            # 讀取資料
            df_ee_bom = pd.read_excel(excel_file, sheet_name="EE_BOM")
            df_cost_adder = pd.read_excel(excel_file, sheet_name="Cost_Adder_Logistic")
            
            # 加入 Project_Name 欄位
            df_ee_bom.insert(0, "Project_Name", project_name)
            df_cost_adder.insert(0, "Project_Name", project_name)
            
            # 轉換 Effective_Start_Date 為 Quarter
            quarter_value = None
            if "Effective_Start_Date" in df_ee_bom.columns:
                # 取第一筆有效的日期來轉換
                for date_val in df_ee_bom["Effective_Start_Date"]:
                    q = date_to_quarter(date_val)
                    if q:
                        quarter_value = q
                        break
                
                df_ee_bom["Quarter"] = quarter_value
                st.write(f"**轉換後的 Quarter：** `{quarter_value}`")
            else:
                st.warning("⚠️ EE_BOM 中沒有 Effective_Start_Date 欄位")
                df_ee_bom["Quarter"] = None
            
            # Cost_Adder_Logistic 的 Quarter 來自 EE_BOM
            # 根據 Parent_DPN 匹配（取第一筆匹配的）
            if "Parent_DPN" in df_cost_adder.columns and "PARENT_DPN" in df_ee_bom.columns:
                # 建立 PARENT_DPN -> Quarter 的對應
                dpn_quarter_map = df_ee_bom.groupby("PARENT_DPN")["Quarter"].first().to_dict()
                df_cost_adder["Quarter"] = df_cost_adder["Parent_DPN"].map(dpn_quarter_map)
            else:
                df_cost_adder["Quarter"] = quarter_value
            
            # 顯示預覽
            st.write("---")
            st.subheader("👀 資料預覽")
            
            tab1, tab2 = st.tabs(["EE_BOM", "Cost_Adder_Logistic"])
            
            with tab1:
                st.write(f"共 {len(df_ee_bom)} 筆資料")
                st.dataframe(df_ee_bom.head(10), use_container_width=True)
            
            with tab2:
                st.write(f"共 {len(df_cost_adder)} 筆資料")
                st.dataframe(df_cost_adder.head(10), use_container_width=True)
            
            # 上傳按鈕
            st.write("---")
            if st.button("✅ 確認上傳", type="primary", use_container_width=True):
                with st.spinner("正在處理資料..."):
                    # 儲存到資料庫
                    inserted_ee = insert_data("EE_BOM", df_ee_bom)
                    inserted_cost = insert_data("Cost_Adder_Logistic", df_cost_adder)
                    
                    # 更新 metadata
                    refresh_metadata()
                
                # 顯示結果
                st.success("✅ 上傳完成！")
                
                col1, col2 = st.columns(2)
                with col1:
                    st.metric(
                        label="EE_BOM",
                        value=f"{inserted_ee} 筆新增",
                        delta=f"共 {len(df_ee_bom)} 筆（{len(df_ee_bom) - inserted_ee} 筆重複）"
                    )
                with col2:
                    st.metric(
                        label="Cost_Adder_Logistic",
                        value=f"{inserted_cost} 筆新增",
                        delta=f"共 {len(df_cost_adder)} 筆（{len(df_cost_adder) - inserted_cost} 筆重複）"
                    )
        
        except Exception as e:
            st.error(f"❌ 讀取檔案時發生錯誤：{str(e)}")


def report_page():
    """產生報表頁面"""
    st.header("📊 產生報表")
    
    # 載入 metadata
    metadata = load_metadata()
    
    # 選擇 Table
    table_options = ["EE_BOM", "Cost_Adder_Logistic"]
    selected_table = st.selectbox(
        "選擇資料表",
        table_options,
        help="選擇要產生報表的資料表"
    )
    
    # 檢查是否有資料
    if selected_table not in metadata or not metadata[selected_table]:
        st.warning(f"⚠️ {selected_table} 尚無資料，請先上傳檔案。")
        return
    
    st.write("---")
    st.subheader("🔍 篩選條件")
    st.caption("可複選，欄位內為 OR 邏輯，欄位間為 AND 邏輯")
    
    # 動態產生篩選條件
    filters = {}
    table_metadata = metadata[selected_table]
    
    # 將篩選條件分成多欄顯示
    columns = list(table_metadata.keys())
    num_cols = 3
    
    for i in range(0, len(columns), num_cols):
        cols = st.columns(num_cols)
        for j, col in enumerate(columns[i:i+num_cols]):
            with cols[j]:
                options = table_metadata.get(col, [])
                selected = st.multiselect(
                    col,
                    options=options,
                    default=[],
                    help=f"選擇 {col} 的篩選值（可複選）"
                )
                filters[col] = selected
    
    # 查詢與下載
    st.write("---")

    result_df = pd.DataFrame()
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        preview_clicked = st.button("👁️ 預覽資料", use_container_width=True)
    
    # 顯示預覽
    if preview_clicked or "preview_shown" in st.session_state:
        result_df = query_data(selected_table, filters)

        st.write("---")
        st.subheader("📋 資料預覽")
        
        if result_df.empty:
            st.info("🔍 無符合篩選條件的資料")
        else:
            st.write(f"共 {len(result_df)} 筆資料（顯示前 100 筆）")
            st.dataframe(result_df.head(100), use_container_width=True)
            st.session_state["preview_shown"] = True

    with col2:
        # 先查詢資料以便產生下載檔案
        
        if not result_df.empty:
            # 產生 Excel 檔案
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                result_df.to_excel(writer, sheet_name=selected_table, index=False)
            excel_data = output.getvalue()
            
            # 下載檔案名稱
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{selected_table}_report_{timestamp}.xlsx"
            
            st.download_button(
                label="⬇️ 下載 Excel 報表",
                data=excel_data,
                file_name=filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                type="primary"
            )
        else:
            st.button(
                "⬇️ 下載 Excel 報表",
                disabled=True,
                use_container_width=True,
                help="無符合條件的資料"
            )


# =============================================================================
# 主程式入口
# =============================================================================
if __name__ == "__main__":
    main()