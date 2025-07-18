import streamlit as st
import sqlite3
import pandas as pd
import json
from datetime import datetime
from st_aggrid import AgGrid, GridOptionsBuilder

# --- CONFIG ---
st.set_page_config(page_title="Real Time Recon", layout="wide")

DB_PATH = "/home/pscadmin/recon/recon_recon.db"
STATUS_PATH = "/home/pscadmin/recon/process_status.json"

@st.cache_data(ttl=300)
def load_data():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(f"SELECT * FROM merged_data", conn)
    conn.close()
    return df

def load_shop_open_lines():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM shop_location_normalized", conn)
    conn.close()
    return df

def load_status():
    try:
        with open(STATUS_PATH, "r") as f:
            return json.load(f)
    except Exception:
        return None

# --- TITLE ---
st.title("Real Time Recon Dashboard")

# --- STATUS CARD ---
status = load_status()
now = datetime.now()

if not status:
    st.error("⚠️ Could not load sync status.")
else:
    start_time = datetime.fromisoformat(status["start_time"])
    end_time = datetime.fromisoformat(status["end_time"]) if status.get("end_time") else None

    if status["status"] == "running":
        elapsed = now - start_time
        st.warning(f"🟡 Aging sync is currently running... ({elapsed.seconds // 60} min)")
        st.code(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    elif status["status"] == "complete":
        duration = end_time - start_time
        st.success(f"✅ Last sync completed at {end_time.strftime('%Y-%m-%d %H:%M:%S')} (took {duration.seconds // 60} min)")
        st.code(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    elif status["status"] == "failed":
        st.error(f"❌ Sync failed during: `{status.get('failed_script', 'unknown')}`")
        if end_time:
            duration = end_time - start_time
            st.text(f"Failed after {duration.seconds // 60} min.")
        with st.expander("Show error traceback"):
            st.text(status.get("error", "No error message available."))

# --- LOAD DATA ---
df = load_data()

if df.empty:
    st.warning("No data found in merged_data table.")
    st.stop()

# --- RADIO TABS ---
tab_choice = st.radio("Select View", [
    "Open Shopify Lines",
    "Discrepancies", 
    "ERP Discrepancies", 
    "OMS Discrepancies", 
    "Aging All", 
    "Backorders", 
    "Ready to Ship", 
    "Aging Missing OMS", 
    "Shopify Aging ERP"
], horizontal=True)

# --- COLUMN CONFIG ---
final_columns = [
    "shop_order_name", "shop_order_id", "shop_omsLocation", "Shop Date", "Shop Time", "OMS Released", "oms_state",
    "shop_fdm4_order_number", "shop_sku", "shop_line_item_name", "shop_ordered_quantity", "shop_quantity_assigned",
    "match_erp", "match_oms", "is_gwp_line", "erp_OrderNumber", "erp_OrderWarehouse", "erp_LineWarehouse",
    "erp_WarehouseStatus", "erp_Reviewed", "erp_Approved", "erp_Complete", "erp_OrderQty", "erp_ShippedQty", "erp_CancelQty",
]

rename_map = {
    "shop_order_name": "Shopify Order", "shop_order_id": "Shopify ID", "shop_omsLocation": "Location",
    "Shop Date": "Shopify Order Date", "Shop Time": "Shopify Order Time", "oms_state": "OMS Status",
    "shop_fdm4_order_number": "OMS Order Number", "shop_sku": "Item Number", "shop_line_item_name": "Item Name",
    "shop_ordered_quantity": "Shopify Order Qty", "shop_quantity_assigned": "Shopify Open Qty", "match_erp": "ERP Match",
    "match_oms": "OMS Match", "is_gwp_line": "GWP", "erp_OrderNumber": "ERP Order Number", "erp_OrderWarehouse": "Order Warehouse",
    "erp_LineWarehouse": "Order-Line Warehouse", "erp_WarehouseStatus": "Warehouse Status", "erp_Reviewed": "Released From Hold",
    "erp_Approved": "Approved", "erp_Complete": "Confirmed", "erp_OrderQty": "ERP Order Qty", "erp_ShippedQty": "ERP Shipped Qty",
    "erp_CancelQty": "ERP Cancel Qty",
}

# --- HELPERS ---
def prepare_df(base_df):
    base_df["Date"] = pd.to_datetime(base_df["created_at_pst"]).dt.strftime("%m/%d/%Y")
    base_df["Time"] = pd.to_datetime(base_df["created_at_pst"]).dt.strftime("%H:%M:%S")
    shop_dt = pd.to_datetime(base_df["shop_created_at"], utc=True)
    shop_dt_pst = shop_dt - pd.Timedelta(hours=7)
    base_df["Shop Date"] = shop_dt_pst.dt.strftime("%m/%d/%Y")
    base_df["Shop Time"] = shop_dt_pst.dt.strftime("%H:%M:%S")
    base_df["OMS Released"] = base_df["shop_fdm4_order_number"].apply(lambda x: "Yes" if pd.notna(x) and str(x).strip() != "" else "No")
    return base_df

def display_aggrid(fdf, subtitle):
    gb = GridOptionsBuilder.from_dataframe(fdf)
    gb.configure_default_column(filter="agTextColumnFilter", sortable=True, resizable=True)

    if "ERP Match" in fdf.columns:
        gb.configure_column("ERP Match", type=["leftAligned"])
    if "OMS Match" in fdf.columns:
        gb.configure_column("OMS Match", type=["leftAligned"])

    gb.configure_default_column(minWidth=100)
    gb.configure_grid_options(
        statusBar={
            "statusPanels": [
                {"statusPanel": "agTotalRowCountComponent", "align": "left"},
                {"statusPanel": "agFilteredRowCountComponent"},
                {"statusPanel": "agSelectedRowCountComponent"},
                {"statusPanel": "agAggregationComponent"}
            ]
        }
    )

    grid_options = gb.build()
    st.subheader(f"{subtitle} ({len(fdf):,} records)")

    AgGrid(
        fdf,
        gridOptions=grid_options,
        use_container_width=True,
        height=500,
        enable_enterprise_modules=True,
        key=subtitle
    )

# --- TAB FILTERING ---
if tab_choice == "Discrepancies":
    st.caption("⚖️ All records not fully matched across systems (Shopify, OMS, ERP). (GWP items excluded)")
    fdf = df[(df["match_all"] != 1) & (df["is_gwp_line"].fillna("").astype(str) != "1")].copy()
elif tab_choice == "ERP Discrepancies":
    st.caption("📄 Records missing ERP match (requiring fulfillment ID). (GWP items excluded)")
    fdf = df[(df["match_erp"] == 0) & 
             (df["is_gwp_line"].fillna("").astype(str) != "1") & 
             (df["oms_fulfillmentId"].notna()) & 
             (df["oms_fulfillmentId"].str.strip() != "")].copy()
elif tab_choice == "OMS Discrepancies":
    st.caption("📄 Records missing OMS match (requiring fulfillment ID). (GWP items excluded)")
    fdf = df[(df["match_oms"] == 0) & 
             (df["is_gwp_line"].fillna("").astype(str) != "1") & 
             (df["oms_fulfillmentId"].notna()) & 
             (df["oms_fulfillmentId"].str.strip() != "")].copy()
elif tab_choice == "Aging All":
    st.caption("⏳ Orders older than 12 hours, regardless of warehouse or OMS status. (GWP items excluded)")
    shop_dt = pd.to_datetime(df["shop_created_at"], utc=True)
    fdf = df[(shop_dt < (pd.Timestamp.utcnow() - pd.Timedelta(hours=12))) & 
             (df["is_gwp_line"].fillna("").astype(str) != "1")].copy()
elif tab_choice == "Backorders":
    st.caption("📦 Orders older than 12 hours with ERP status 'Backorder'. (GWP items excluded)")
    shop_dt = pd.to_datetime(df["shop_created_at"], utc=True)
    fdf = df[(shop_dt < (pd.Timestamp.utcnow() - pd.Timedelta(hours=12))) & 
             (df["erp_WarehouseStatus"].fillna("").str.strip() == "Backorder") & 
             (df["is_gwp_line"].fillna("").astype(str) != "1")].copy()
elif tab_choice == "Ready to Ship":
    st.caption("🏬 Orders older than 12 hours with ERP status 'Ready'. (GWP items excluded)")
    shop_dt = pd.to_datetime(df["shop_created_at"], utc=True)
    fdf = df[(shop_dt < (pd.Timestamp.utcnow() - pd.Timedelta(hours=12))) & 
             (df["erp_WarehouseStatus"].fillna("").str.strip() == "Ready") & 
             (df["is_gwp_line"].fillna("").astype(str) != "1")].copy()
elif tab_choice == "Aging Missing OMS":
    st.caption("🌀 Orders older than 12 hours missing OMS order number. (GWP items excluded)")
    shop_dt = pd.to_datetime(df["shop_created_at"], utc=True)
    fdf = df[(shop_dt < (pd.Timestamp.utcnow() - pd.Timedelta(hours=12))) & 
             ((df["shop_fdm4_order_number"].isna()) | (df["shop_fdm4_order_number"].str.strip() == "")) & 
             (df["is_gwp_line"].fillna("").astype(str) != "1")].copy()
elif tab_choice == "Shopify Aging ERP":
    st.caption("🏭 Orders older than 12 hours with OMS order number, but ERP status not 'Backorder' or 'Ready'. (GWP items excluded)")
    shop_dt = pd.to_datetime(df["shop_created_at"], utc=True)
    fdf = df[(shop_dt < (pd.Timestamp.utcnow() - pd.Timedelta(hours=12))) & 
             (df["shop_fdm4_order_number"].notna()) & (df["shop_fdm4_order_number"].str.strip() != "") & 
             (df["erp_WarehouseStatus"].fillna("").str.strip() != "Backorder") & 
             (df["erp_WarehouseStatus"].fillna("").str.strip() != "Ready") & 
             (df["is_gwp_line"].fillna("").astype(str) != "1")].copy()

elif tab_choice == "Open Shopify Lines":
    st.caption("🛒 Open lines from Shopify not yet reconciled. Pulled directly from `shop_open_lines` table. (GWP items excluded)")
    fdf = df[(df["is_gwp_line"].fillna("").astype(str) != "1")]


else:
    fdf = pd.DataFrame()

# Display the final table if data exists
if not fdf.empty:
    if tab_choice != "Open Shopify Lines":
        fdf = prepare_df(fdf)
        fdf = fdf[final_columns].rename(columns=rename_map)
    display_aggrid(fdf, tab_choice)

