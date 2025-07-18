# 🧠 Order Recon Aging System

This repository automates a real-time order aging and reconciliation pipeline across multiple systems—**Shopify**, **JDA OMS**, and **ERP**—into a unified SQLite database for daily reporting and discrepancy resolution.

The system integrates API data, normalizes location references, performs layered merging, and flags aging issues or fulfillment mismatches using advanced logic.

---

## 📌 Key Features

- 🛒 Pulls **unfulfilled Shopify order lines** via GraphQL API
- 📦 Fetches **JDA OMS order lines** for various states (RELEASED, PICKED, etc.)
- 🏭 Reads **ERP open order lines** for fulfillment status
- 🔄 Reconciles and merges all sources into `merged_data` table
- 🧮 Calculates `recon_state` for every order line using business rules
- 📊 Generates aging report via `erp_missing_aging` for open ERP mismatches
- 📅 Visualizes aging by day in a **Streamlit dashboard**

---

## 🗂️ Project Structure

```
.
├── REAL_TIME_AGING.py             # 🔁 Orchestrates all core scripts in sequence
├── shop_open_released.py          # 📥 Pulls Shopify unfulfilled orders via GraphQL
├── oms_open_lines.py              # 📦 Fetches JDA OMS lines by order state
├── shop_loc_norm.py               # 🧹 Normalizes shop/oms/erp joins → shop_location_normalized
├── merged_open_lines.py           # 🧠 Builds merged_data with match_erp/match_oms flags
├── shop_loc_norm_recon.py         # 🧪 Adds recon_state logic → shop_location_reconciled
├── erp_missing_aging.py           # 📈 Tracks unresolved ERP matches over time
├── app_by_day.py                  # 📊 Streamlit app with daily aging dashboard
├── recon_recon.db                 # 🗃️ Target SQLite DB (created by scripts)
└── .env                           # 🔐 API keys and credentials (not included here)
```

---

## 🧪 Reconciliation Flow

1. **`shop_open_released.py`**: Pulls unfulfilled Shopify orders and loads `shop_open_lines`.
2. **`oms_open_lines.py`**: Retrieves JDA OMS open lines by state into `oms_open_lines`.
3. **`shop_loc_norm.py`**: Normalizes locations and merges with ERP to produce `shop_location_normalized`.
4. **`merged_open_lines.py`**: Builds `merged_data` with match flags.
5. **`shop_loc_norm_recon.py`**: Adds business logic for `recon_state` into `shop_location_reconciled`.
6. **`erp_missing_aging.py`**: Tracks long-term unmatched ERP lines into `erp_missing_aging`.

---

## 📊 Streamlit Dashboard

Run the dashboard:

```bash
streamlit run app_by_day.py
```

- Tabs for last 7 days + older
- Exportable daily CSVs
- Shows latest process status

---

## 🚀 Run the Pipeline

```bash
python3 REAL_TIME_AGING.py
```

Runs the full data ingestion, reconciliation, and aging pipeline. Status is saved to `/home/pscadmin/recon/process_status.json`.

---

## 🔐 Environment Variables

Create a `.env` file:

```
JDA_CLIENT_ID=your_client_id
JDA_CLIENT_SECRET=your_secret
JDA_SCOPE=https://your-scope-url
```

---

## 📦 Dependencies

```bash
pip install -r requirements.txt
```

Likely packages:

```
requests
python-dotenv
pytz
pandas
streamlit
st-aggrid
```

---

## ✅ Output Tables

| Table Name                  | Description                                   |
|----------------------------|-----------------------------------------------|
| `shop_open_lines`          | Raw Shopify data                              |
| `oms_open_lines`           | JDA OMS lines                                 |
| `shop_location_normalized` | Combined view of shop/oms/erp                 |
| `merged_data`              | Source-of-truth with match flags              |
| `shop_location_reconciled` | Adds `recon_state` analysis                   |
| `erp_missing_aging`        | Tracks unmatched ERP lines over time          |
