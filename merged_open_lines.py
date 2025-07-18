from datetime import datetime
import pytz
import sqlite3

DB_PATH = "recon_recon.db"

def get_pacific_timestamp():
    pacific = pytz.timezone("America/Los_Angeles")
    now_pacific = datetime.now(pacific)
    return now_pacific.isoformat()

def setup_merged_table(conn, cursor, shop_cols, oms_cols, erp_cols):
    # Check if table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='merged_data'")
    exists = cursor.fetchone()

    if exists:
        print("[ℹ️] merged_data table already exists — skipping creation and preserving data.")
        return

    # Build CREATE TABLE statement dynamically
    all_columns = {
        "shop_order_name": "TEXT",
        "shop_sku": "TEXT",
        "shop_omsLocation": "TEXT",
        "shop_line_item_id": "TEXT",
        "created_at_pst": "TEXT",
        "match_erp": "INTEGER",
        "match_oms": "INTEGER",
        "match_all": "INTEGER",
    }

    for col in shop_cols:
        all_columns[col] = "TEXT"
    for col in oms_cols:
        all_columns[col] = "TEXT"
    for col in erp_cols:
        all_columns[col] = "TEXT"

    columns_defs = ",\n".join([f'"{col}" {dtype}' for col, dtype in all_columns.items()])
    columns_defs += ",\nPRIMARY KEY (shop_order_name, shop_sku, shop_omsLocation, shop_line_item_id)"

    cursor.execute(f"CREATE TABLE merged_data ({columns_defs})")
    conn.commit()
    print("[✅] merged_data table created (first time only).")

def build_merged_data():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Get columns from source tables
    cursor.execute("PRAGMA table_info(shop_location_normalized)")
    shop_cols = [col[1] for col in cursor.fetchall()]

    cursor.execute("PRAGMA table_info(oms_open_lines)")
    oms_cols = [col[1] for col in cursor.fetchall()]

    cursor.execute("PRAGMA table_info(erp_open_lines)")
    erp_cols = [col[1] for col in cursor.fetchall()]

    setup_merged_table(conn, cursor, shop_cols, oms_cols, erp_cols)

    # Get normalized base rows
    cursor.execute("SELECT * FROM shop_location_normalized")
    shop_rows = cursor.fetchall()
    shop_desc = [col[0] for col in cursor.description]

    shop_keys = set((row[shop_desc.index("shop_order_name")], row[shop_desc.index("shop_sku")], row[shop_desc.index("shop_omsLocation")], row[shop_desc.index("shop_line_item_id")]) for row in shop_rows)

    # Existing merged keys
    cursor.execute("SELECT shop_order_name, shop_sku, shop_omsLocation, shop_line_item_id FROM merged_data")
    merged_rows = cursor.fetchall()
    merged_keys = set(merged_rows)

    record_counter = 0

    for shop_row in shop_rows:
        key = (
            shop_row[shop_desc.index("shop_order_name")],
            shop_row[shop_desc.index("shop_sku")],
            shop_row[shop_desc.index("shop_omsLocation")],
            shop_row[shop_desc.index("shop_line_item_id")]
        )

        # Fetch OMS row
        cursor.execute("""
            SELECT * FROM oms_open_lines
            WHERE oms_orderName = ? AND oms_productID = ? AND oms_location = ?
        """, (key[0], key[1], key[2]))
        oms_row = cursor.fetchone()
        oms_desc = [col[0] for col in cursor.description] if oms_row else []

        # Fetch ERP row
        cursor.execute("""
            SELECT * FROM erp_open_lines
            WHERE erp_CustOrderNum = ? AND erp_ItemNumber = ? AND erp_LineWarehouse = ?
        """, (key[0], key[1], key[2]))
        erp_row = cursor.fetchone()
        erp_desc = [col[0] for col in cursor.description] if erp_row else []

        match_erp = 1 if erp_row else 0
        match_oms = 1 if oms_row else 0
        match_all = 1 if erp_row and oms_row else 0

        merged_record = {
            "shop_order_name": key[0],
            "shop_sku": key[1],
            "shop_omsLocation": key[2],
            "shop_line_item_id": key[3],
            "created_at_pst": get_pacific_timestamp() if key not in merged_keys else None,
            "match_erp": match_erp,
            "match_oms": match_oms,
            "match_all": match_all,
        }

        # Add all shop columns
        for i, col in enumerate(shop_desc):
            merged_record[col] = shop_row[i]

        # Add all OMS columns
        if oms_row:
            for i, col in enumerate(oms_desc):
                merged_record[col] = oms_row[i]

        # Add all ERP columns
        if erp_row:
            for i, col in enumerate(erp_desc):
                merged_record[col] = erp_row[i]

        # Columns to insert
        columns = list(merged_record.keys())
        values = [merged_record[c] for c in columns]

        placeholders = ", ".join(["?"] * len(columns))
        columns_str = ", ".join([f'"{c}"' for c in columns])

        # Try update first
        update_columns = ", ".join([f'"{c}" = ?' for c in columns if c != "created_at_pst"])
        update_values = [merged_record[c] for c in columns if c != "created_at_pst"] + list(key)
        cursor.execute(f"""
            UPDATE merged_data SET {update_columns}
            WHERE shop_order_name = ? AND shop_sku = ? AND shop_omsLocation = ? AND shop_line_item_id = ?
        """, update_values)

        if cursor.rowcount == 0:
            cursor.execute(f"""
                INSERT INTO merged_data ({columns_str})
                VALUES ({placeholders})
            """, values)

        record_counter += 1
        if record_counter % 1000 == 0:
            print(f"[+] Processed {record_counter} records...")

    print(f"[✓] Total records processed: {record_counter}")

    # Delete merged records no longer in Shopify
    keys_to_delete = merged_keys - shop_keys
    for k in keys_to_delete:
        cursor.execute("""
            DELETE FROM merged_data
            WHERE shop_order_name = ? AND shop_sku = ? AND shop_omsLocation = ? AND shop_line_item_id = ?
        """, k)
        print(f"[-] Deleted merged record no longer in Shopify: {k}")

    conn.commit()
    conn.close()
    print("[✅] Merged data table built and synced.")

if __name__ == "__main__":
    build_merged_data()
