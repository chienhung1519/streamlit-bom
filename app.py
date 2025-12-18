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
        "Effective_Start_Date",
        "Effective_End_Date",
    ],
    "Cost_Adder_Logistic": [
        "Project_Name",
        "Parent_DPN",
        "Sub_Cost_Category",
        "Region",
    ],
}

# =============================================================================
# 資料庫操作
# =============================================================================
def get_db_connection():
    """取得資料庫連線"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_database():
    """初始化資料庫（如果 table 不存在則建立）"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 檢查 table 是否存在，若不存在則在第一次上傳時動態建立
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


def create_table_from_df(table_name: str, df: pd.DataFrame):
    """根據 DataFrame 動態建立 table"""
    conn = get_db_connection()
    
    # 加入 created_at 欄位
    df_with_timestamp = df.copy()
    df_with_timestamp["created_at"] = datetime.now().isoformat()
    
    # 建立 table（如果不存在）
    df_with_timestamp.head(0).to_sql(table_name, conn, if_exists="ignore", index=False)
    
    conn.close()


def get_existing_data(table_name: str) -> pd.DataFrame:
    """取得 table 中所有資料"""
    if not table_exists(table_name):
        return pd.DataFrame()
    
    conn = get_db_connection()
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    conn.close()
    return df


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
    範例：(Dell) SEBOM_Foxconn_Boss S2_PROD_Quote_20250411.xlsx → Boss S2
    """
    # 移除副檔名
    name_without_ext = Path(filename).stem
    
    # 以底線分隔
    parts = name_without_ext.split("_")
    
    if len(parts) >= 3:
        return parts[2]
    else:
        return name_without_ext  # 如果格式不符，回傳整個檔名


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
    
    # 側邊欄選單
    page = st.sidebar.radio(
        "功能選擇",
        ["上傳資料", "產生報表"],
        index=0
    )
    
    if page == "上傳資料":
        upload_page()
    else:
        report_page()


def upload_page():
    """上傳資料頁面"""
    st.header("📤 上傳資料")
    
    st.info("""
    **使用說明：**
    1. 上傳 Excel 檔案（.xlsx）
    2. 檔名格式範例：`(Dell) SEBOM_Foxconn_Boss S2_PROD_Quote_20250411.xlsx`
    3. 系統會自動讀取 `EE_BOM` 和 `Cost_Adder_Logistic` 兩個 Sheet
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
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        preview_clicked = st.button("👁️ 預覽資料", use_container_width=True)
    
    with col2:
        # 先查詢資料以便產生下載檔案
        result_df = query_data(selected_table, filters)
        
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
    
    # 顯示預覽
    if preview_clicked or "preview_shown" in st.session_state:
        st.write("---")
        st.subheader("📋 資料預覽")
        
        if result_df.empty:
            st.info("🔍 無符合篩選條件的資料")
        else:
            st.write(f"共 {len(result_df)} 筆資料（顯示前 100 筆）")
            st.dataframe(result_df.head(100), use_container_width=True)
            st.session_state["preview_shown"] = True


# =============================================================================
# 主程式入口
# =============================================================================
if __name__ == "__main__":
    init_database()
    main()