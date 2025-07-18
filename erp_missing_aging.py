from datetime import datetime
import pytz
import sqlite3

DB_PATH = "recon_recon.db"

def get_current_missing_erp():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # ✅ Directly query merged_data with match_erp = 0
    cursor.execute("SELECT * FROM merged_data WHERE match_erp = 0")
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    conn.close()
    return [dict(zip(columns, row)) for row in rows], columns

def ensure_aging_table_columns(columns):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Get columns from merged_data dynamically
    cursor.execute("PRAGMA table_info(merged_data)")
    merged_columns_info = cursor.fetchall()
    merged_column_names = [col[1] for col in merged_columns_info]

    # Only keep columns with prefixes (shop_, oms_, erp_, match_)
    valid_columns = [c for c in merged_column_names if c.startswith(("shop_", "oms_", "erp_", "match_"))]

    columns_str = ", ".join([f'"{c}"' for c in valid_columns])

    # Drop aging table if exists to rebuild schema cleanly
    cursor.execute("DROP TABLE IF EXISTS erp_missing_aging")

    # Create table using explicit columns
    cursor.execute(f"""
        CREATE TABLE erp_missing_aging AS
        SELECT {columns_str}, '' AS last_update FROM merged_data WHERE 0
    """)
    conn.commit()

    # Get current columns in aging table
    cursor.execute("PRAGMA table_info(erp_missing_aging)")
    aging_columns_info = cursor.fetchall()
    aging_column_names = [col[1] for col in aging_columns_info]

    # Add missing columns dynamically
    for col in columns:
        if col not in aging_column_names and col in valid_columns:
            cursor.execute(f'ALTER TABLE erp_missing_aging ADD COLUMN "{col}" TEXT')
            print(f"[+] Added missing column to aging: {col}")

    # ✅ Fixed: check column names only
    if "last_update" not in aging_column_names:
        cursor.execute("ALTER TABLE erp_missing_aging ADD COLUMN last_update TEXT")
        print("[+] Added last_update column to aging")

    conn.commit()
    conn.close()

def sync_missing_erp_to_aging():
    # Get current records and columns from merged_data
    current_records, view_columns = get_current_missing_erp()

    # Ensure aging table columns match
    ensure_aging_table_columns(view_columns)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Get current keys from aging
    cursor.execute("SELECT shop_order_name, shop_sku, shop_omsLocation, shop_line_item_id FROM erp_missing_aging")
    aging_rows = cursor.fetchall()
    aging_keys = set(aging_rows)

    current_keys = set(
        (r["shop_order_name"], r["shop_sku"], r["shop_omsLocation"], r["shop_line_item_id"]) for r in current_records
    )

    # Setup Pacific timezone
    pacific = pytz.timezone("America/Los_Angeles")
    now_pacific = datetime.now(pacific)
    now_str = now_pacific.isoformat()

    for record in current_records:
        keys = (record["shop_order_name"], record["shop_sku"], record["shop_omsLocation"], record["shop_line_item_id"])

        cursor.execute("""
            SELECT * FROM erp_missing_aging
            WHERE shop_order_name = ? AND shop_sku = ? AND shop_omsLocation = ? AND shop_line_item_id = ?
        """, keys)
        row = cursor.fetchone()

        # Only use columns that actually exist in the aging table
        cursor.execute("PRAGMA table_info(erp_missing_aging)")
        aging_columns_info = cursor.fetchall()
        aging_column_names = [col[1] for col in aging_columns_info]

        valid_columns = {k: v for k, v in record.items() if k in aging_column_names}

        if row is None:
            columns_str = ', '.join([f'"{k}"' for k in valid_columns.keys()] + ['"last_update"'])
            placeholders = ', '.join(['?'] * len(valid_columns) + ['?'])
            values = list(valid_columns.values()) + [now_str]
            cursor.execute(f"""
                INSERT INTO erp_missing_aging ({columns_str})
                VALUES ({placeholders})
            """, values)
            print(f"[+] Inserted new record: {keys}")
        else:
            db_record = dict(zip([col[0] for col in cursor.description], row))
            changed = False
            for k in valid_columns:
                if str(valid_columns[k]) != str(db_record.get(k)):
                    changed = True
                    break

            if changed:
                set_clause = ', '.join([f'"{k}" = ?' for k in valid_columns.keys()])
                values = list(valid_columns.values()) + list(keys)
                cursor.execute(f"""
                    UPDATE erp_missing_aging
                    SET {set_clause}
                    WHERE shop_order_name = ? AND shop_sku = ? AND shop_omsLocation = ? AND shop_line_item_id = ?
                """, values)
                print(f"[~] Updated record without changing last_update: {keys}")
            else:
                print(f"[=] No change for: {keys}")

    # Delete resolved records
    keys_to_delete = aging_keys - current_keys
    for k in keys_to_delete:
        cursor.execute("""
            DELETE FROM erp_missing_aging
            WHERE shop_order_name = ? AND shop_sku = ? AND shop_omsLocation = ? AND shop_line_item_id = ?
        """, k)
        print(f"[-] Deleted resolved record: {k}")

    conn.commit()
    conn.close()
    print("[✅] ERP missing aging table synced and cleaned.")

if __name__ == "__main__":
    sync_missing_erp_to_aging()
