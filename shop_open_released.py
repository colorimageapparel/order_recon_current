import sqlite3
import requests
import os
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURATION ---
SHOP_NAME = "alo-yoga"
ACCESS_TOKEN = "7904b3cc654fa017c25b62f8c16bc6fc"  # Replace with your actual token

GRAPHQL_URL = f"https://{SHOP_NAME}.myshopify.com/admin/api/2024-04/graphql.json"

HEADERS = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": ACCESS_TOKEN
}

DB_NAME = "recon_recon.db"
CURSOR_FILE = "last_cursor.txt"
RESET_CURSOR = False

# --- GRAPHQL QUERY ---
def query_unfulfilled_lines(cursor=None):
    created_at_filter = "2025-07-06T00:00:00"

    query = f"""
    query ($cursor: String) {{
      orders(first: 250, after: $cursor, query: "created_at:>={created_at_filter} (fulfillment_status:unfulfilled OR fulfillment_status:partial)") {{
        edges {{
          cursor
          node {{
            id
            name
            createdAt
            cancelledAt
            closedAt
            displayFinancialStatus
            metafield(namespace: "FDM4", key: "fdm4_order_number") {{
              value
            }}
            fulfillmentOrders(first: 10) {{
              edges {{
                node {{
                  id
                  assignedLocation {{
                    name
                  }}
                  lineItems(first: 20) {{
                    edges {{
                      node {{
                        id
                        lineItem {{
                          id
                          name
                          sku
                          quantity
                        }}
                        remainingQuantity
                      }}
                    }}
                  }}
                }}
              }}
            }}
          }}
        }}
        pageInfo {{
          hasNextPage
        }}
      }}
    }}
    """
    variables = {"cursor": cursor}

    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)

    response = session.post(GRAPHQL_URL, headers=HEADERS, json={"query": query, "variables": variables}, timeout=30)

    if response.status_code != 200:
        print("Error:", response.text)
        return None

    return response.json()

# --- DB SETUP ---
def setup_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS shop_open_lines")
    conn.commit()
    print("[🗑️] Dropped existing shop_open_lines table to rebuild schema.")

    cursor.execute("""
        CREATE TABLE shop_open_lines (
            shop_order_name TEXT,
            shop_order_id TEXT,
            shop_created_at TEXT,
            shop_fdm4_order_number TEXT,
            shop_fulfillment_order_id TEXT,
            shop_assigned_location TEXT,
            shop_line_item_id TEXT,
            shop_line_item_name TEXT,
            shop_sku TEXT,
            shop_ordered_quantity INTEGER,
            shop_quantity_assigned INTEGER,
            shop_actual_location TEXT,
            is_gwp_line INTEGER DEFAULT 0,
            is_preorder INTEGER DEFAULT 0,
            shop_preorder_ship_date TEXT,
            PRIMARY KEY (shop_order_id, shop_line_item_id)
        )
    """)
    conn.commit()
    conn.close()
    print("[✅] Recreated shop_open_lines table with preorder fields (defaulted).")

# --- DB INSERT ---
def insert_data(lines):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    for line in lines:
        sku = line["shop_sku"]
        is_gwp_line = 1 if str(sku).startswith("ALOGWP") or str(sku).startswith("LOYALTY") else 0

        cursor.execute("""
            INSERT OR REPLACE INTO shop_open_lines (
                shop_order_name, shop_order_id, shop_created_at, shop_fdm4_order_number,
                shop_fulfillment_order_id, shop_assigned_location,
                shop_line_item_id, shop_line_item_name, shop_sku,
                shop_ordered_quantity, shop_quantity_assigned, shop_actual_location,
                is_gwp_line, is_preorder, shop_preorder_ship_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            line["shop_order_name"],
            line["shop_order_id"],
            line["shop_created_at"],
            line["shop_fdm4_order_number"],
            line["shop_fulfillment_order_id"],
            line["shop_assigned_location"],
            line["shop_line_item_id"],
            line["shop_line_item_name"],
            line["shop_sku"],
            line["shop_ordered_quantity"],
            line["shop_quantity_assigned"],
            None,
            is_gwp_line,
            0,        # is_preorder defaulted
            None      # shop_preorder_ship_date defaulted
        ))

    conn.commit()
    conn.close()
    print(f"[✓] Inserted {len(lines)} lines into shop_open_lines.")

# --- MAIN ---
def main():
    setup_db()

    cursor_val = None
    if os.path.exists(CURSOR_FILE):
        with open(CURSOR_FILE, "r") as f:
            cursor_val = f.read().strip()

    if RESET_CURSOR:
        if os.path.exists(CURSOR_FILE):
            os.remove(CURSOR_FILE)
            print("[↩] Cursor file cleared. Starting fresh.")
        cursor_val = None

    page_count = 1
    total_orders = 0
    total_lines = 0
    has_next = True

    while has_next:
        print(f"\n[Batch {page_count}] Fetching orders...")

        data = query_unfulfilled_lines(cursor_val)
        if not data or "data" not in data:
            print("No data or error in response.")
            break

        orders = data["data"]["orders"]["edges"]
        total_orders += len(orders)
        lines_in_batch = 0
        batch_lines = []

        for order_edge in orders:
            order = order_edge["node"]
            if order["displayFinancialStatus"].lower() in ["voided", "refunded", "partially_refunded"]:
                continue
            if order["cancelledAt"] or order["closedAt"]:
                continue

            fdm4_order_number = order["metafield"]["value"] if order.get("metafield") else None
            gid_order = order["id"].split("/")[-1]

            for fo_edge in order["fulfillmentOrders"]["edges"]:
                fo = fo_edge["node"]
                fo_id = fo["id"].split("/")[-1]
                loc_name = fo["assignedLocation"]["name"] if fo["assignedLocation"] else "N/A"
                if loc_name and len(loc_name) >= 3 and loc_name[:3].isdigit():
                    loc_name = f"AYS{loc_name[:3]}"

                for li_edge in fo["lineItems"]["edges"]:
                    li = li_edge["node"]
                    if li["remainingQuantity"] == 0:
                        continue

                    line_item = li["lineItem"]
                    line_item_id = line_item["id"].split("/")[-1]

                    batch_lines.append({
                        "shop_order_name": order["name"],
                        "shop_order_id": gid_order,
                        "shop_created_at": order["createdAt"],
                        "shop_fdm4_order_number": fdm4_order_number,
                        "shop_fulfillment_order_id": fo_id,
                        "shop_assigned_location": loc_name,
                        "shop_line_item_id": line_item_id,
                        "shop_line_item_name": line_item["name"],
                        "shop_sku": line_item["sku"],
                        "shop_ordered_quantity": line_item["quantity"],
                        "shop_quantity_assigned": li["remainingQuantity"]
                    })
                    lines_in_batch += 1

        total_lines += lines_in_batch
        insert_data(batch_lines)

        print(f"[Batch {page_count}] Orders: {len(orders)}, Lines this batch: {lines_in_batch}, Total lines so far: {total_lines}")

        has_next = data["data"]["orders"]["pageInfo"]["hasNextPage"]
        if has_next:
            cursor_val = orders[-1]["cursor"]
            with open(CURSOR_FILE, "w") as f:
                f.write(cursor_val)
            page_count += 1
        else:
            if os.path.exists(CURSOR_FILE):
                os.remove(CURSOR_FILE)
            print("\n[✓] All pages completed. Cursor file cleared.")

    print(f"\n[✓] Inserted total of {total_lines} line items into DB.")
    print(f"[✓] Total orders processed: {total_orders}")

if __name__ == "__main__":
    main()
