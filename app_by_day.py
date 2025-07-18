import streamlit as st
import sqlite3
import pandas as pd
import json
from datetime import datetime, timedelta
from st_aggrid import AgGrid, GridOptionsBuilder
import pytz

# --- CONFIG ---
st.set_page_config(page_title="Shop Location Daily Breakdown", layout="wide")

DB_PATH = "/home/pscadmin/recon/recon_recon.db"
STATUS_PATH = "/home/pscadmin/recon/process_status.json"

# --- LOAD DATA ---
@st.cache_data(ttl=300)
def load_daily_data():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        "SELECT * FROM shop_location_reconciled WHERE is_gwp_line IS NULL OR is_gwp_line != 1",
        conn
    )
    conn.close()
    return df

# --- TIMEZONES ---
utc = pytz.utc
pst = pytz.timezone("America/Los_Angeles")

# --- LOAD STATUS ---
def load_status():
    try:
        with open(STATUS_PATH, "r") as f:
            return json.load(f)
    except Exception:
        return None

# --- TIME PARSE HANDLER ---
def to_aware_utc(dt_raw):
    dt = datetime.fromisoformat(dt_raw)
    return dt if dt.tzinfo else utc.localize(dt)

# --- TITLE ---
st.title("📅 Daily View: Shop Location by Day Reconciled")

# --- REFRESH & STATUS ---
refresh = st.button("🔄 Refresh Data")
status = load_status()
now = datetime.now(tz=utc)

if not status:
    st.error("⚠️ Could not load sync status.")
else:
    start_time_utc = to_aware_utc(status["start_time"])
    end_time_utc = to_aware_utc(status["end_time"]) if status.get("end_time") else None

    start_time_pst = start_time_utc.astimezone(pst)
    end_time_pst = end_time_utc.astimezone(pst) if end_time_utc else None

    if status["status"] == "running":
        elapsed = now - start_time_utc
        st.warning(f"🟡 Aging sync is currently running... ({elapsed.seconds // 60} min)")
        st.code(f"Started at: {start_time_pst.strftime('%Y-%m-%d %I:%M:%S %p %Z')}")

    elif status["status"] == "complete":
        duration = end_time_utc - start_time_utc
        st.success(f"✅ Last sync completed at {end_time_pst.strftime('%Y-%m-%d %I:%M:%S %p %Z')} (took {duration.seconds // 60} min)")
        st.code(f"Started at: {start_time_pst.strftime('%Y-%m-%d %I:%M:%S %p %Z')}")

    elif status["status"] == "failed":
        st.error(f"❌ Sync failed during: `{status.get('failed_script', 'unknown')}`")
        if end_time_pst:
            duration = end_time_utc - start_time_utc
            st.text(f"Failed after {duration.seconds // 60} min.")
        with st.expander("Show error traceback"):
            st.text(status.get("error", "No error message available."))

# --- DATA LOAD ---
df = load_daily_data()
if df.empty:
    st.warning("No data found in `shop_location_normalized` after filtering.")
    st.stop()

# --- DATE PROCESSING ---
df["shop_created_at"] = pd.to_datetime(df["shop_created_at"], errors="coerce", utc=True)
df["created_date"] = df["shop_created_at"].dt.date  # for filtering tabs

# --- COLUMN CONFIG ---
desired_columns = [
    "recon_state", "shop_order_name", "shop_order_id", "shop_created_at",
    "shop_sku", "erp_WarehouseStatus", "shop_ordered_quantity", "shop_quantity_assigned",
    "shop_omsLocation", "oms_fulfillmentId", "erp_preOrder", "shop_preorder_ship_date", "created_date",
    "match_erp", "match_oms", "is_gwp_line"
]

rename_map = {
    "recon_state": "Recon State",
    "shop_order_name": "Shopify Order",
    "shop_order_id": "Shopify ID",
    "shop_created_at": "Order Created At",
    "erp_WarehouseStatus": "ERP Warehouse Status",
    "shop_sku": "SKU",
    "shop_ordered_quantity": "Qty Ordered",
    "shop_quantity_assigned": "Qty Remaining",
    "shop_omsLocation": "Assigned OMS Location",
    "oms_fulfillmentId": "OMS Fulfillment ID",
    "erp_preOrder": "Preorder",
    "shop_preorder_ship_date": "Estimated Ship Date",
    "match_erp": "Match ERP",
    "match_oms": "Match OMS",
    "is_gwp_line": "GWP Item"
}

df = df[[col for col in desired_columns if col in df.columns]].copy()

# --- BUILD TABS ---
today = datetime.utcnow().date()
tab_labels = [(today - timedelta(days=i)).strftime("%a %m/%d") for i in range(7)]
tab_labels.reverse()
tab_labels.append("> 7 Days Old")
tabs = st.tabs(tab_labels)

# --- TAB LOGIC ---
for i, tab in enumerate(tabs):
    with tab:
        if i < 7:
            target_date = today - timedelta(days=(6 - i))
            filtered_df = df[df["created_date"] == target_date]
            st.caption(f"📅 Records for **{target_date.strftime('%A, %B %d')}**")
        else:
            cutoff = today - timedelta(days=7)
            filtered_df = df[df["created_date"] < cutoff]
            st.caption(f"📦 All records older than 7 days (before {cutoff.strftime('%Y-%m-%d')})")

        if filtered_df.empty:
            st.info("No records found for this day.")
        else:
            filtered_df = filtered_df.drop(columns=["created_date"]).rename(columns=rename_map)

            gb = GridOptionsBuilder.from_dataframe(filtered_df)
            gb.configure_default_column(
                filter="agTextColumnFilter",
                sortable=True,
                resizable=True,
                minWidth=100,
                cellStyle={"textAlign": "left"},
                headerClass="left-align-header"
            )
            grid_options = gb.build()

            custom_css = {
                ".left-align-header": {
                    "justify-content": "flex-start !important",
                    "text-align": "left !important"
                }
            }

            st.subheader(f"{len(filtered_df):,} records")

            AgGrid(
                filtered_df,
                gridOptions=grid_options,
                use_container_width=True,
                height=500,
                enable_enterprise_modules=True,
                key=f"aggrid-{i}",
                custom_css=custom_css
            )

            st.caption(f"🔢 **Total Rows:** {len(filtered_df):,}")

            # --- DOWNLOAD CSV BUTTON ---
            csv = filtered_df.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="⬇️ Download CSV for This Tab",
                data=csv,
                file_name=f"shop_location_tab_{i+1}.csv",
                mime="text/csv"
            )
