# Bill of Materials (BOM) made by Streamlit

## 專案結構

```
bom_manager/
├── app.py              # Streamlit 主程式
├── requirements.txt    # 依賴套件
├── database.db         # (執行後自動產生)
└── metadata.json       # (執行後自動產生)
```

## 執行方式

```bash
# 1. 安裝依賴
pip install -r requirements.txt

# 2. 啟動應用程式
streamlit run app.py
```

## 功能摘要

### 上傳資料

* 上傳 `.xlsx` 檔案
* 自動從檔名第 3 個底線區段解析 `Project_Name`
* 讀取 `EE_BOM` 和 `Cost_Adder_Logistic` 兩個 Sheet
* 自動去重（所有欄位相同才視為重複）
* 顯示新增/重複筆數統計

### 產生報表

* 選擇 Table 後動態載入篩選條件
* 多選篩選（欄位內 OR、欄位間 AND）
* 預覽前 100 筆資料
* 下載完整 Excel 報表


## 補充說明

* 資料庫：使用 SQLite，檔案會自動產生在同目錄下
* **Metadata** 排序：每次上傳後重新掃描，unique values 按 `created_at` 由新到舊排序
* 欄位保留：Excel 原始欄位全部保留，額外加入 Project_Name 和 `created_at`