import os
import requests
import sqlite3
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURATION ---
load_dotenv()

SHOP_NAME = os.environ.get("SHOP_NAME")
ACCESS_TOKEN = os.environ.get("SHOPIFY_ACCESS_TOKEN")

GRAPHQL_URL = f"https://{SHOP_NAME}.myshopify.com/admin/api/2024-04/graphql.json"

HEADERS = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": ACCESS_TOKEN
}

DB_NAME = "recon_recon.db"

# --- Setup retry session ---
session = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
adapter = HTTPAdapter(max_retries=retries)
session.mount("https://", adapter)
session.mount("http://", adapter)

# --- Query Shopify by order name ---
def query_order_by_name(order_name):
    query = f"""
    query {{
      orders(first: 1, query: "name:{order_name}") {{
        edges {{
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
      }}
    }}
    """
    response = session.post(GRAPHQL_URL, headers=HEADERS, json={"query": query}, timeout=30)

    if response.status_code != 200:
        print("Error:", response.text)
        return None

    return response.json()

# --- Get missing ERP order names from DB ---
def get_missing_erp_order_names():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT order_name FROM merged_missing_erp_view")
    rows = cursor.fetchall()
    conn.close()
    return [row[0] for row in rows]

# --- Setup local storage table ---
def setup_storage_table():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS shopify_missing_erp_lines (
            order_name TEXT,
            order_id TEXT,
            created_at TEXT,
            fdm4_order_number TEXT,
            fulfillment_order_id TEXT,
            assigned_location TEXT,
            line_item_id TEXT,
            line_item_name TEXT,
            sku TEXT,
            ordered_quantity INTEGER,
            quantity_assigned INTEGER,
            PRIMARY KEY (order_id, line_item_id)
        )
    """)
    conn.commit()
    conn.close()

# --- Insert Shopify lines into storage table ---
def insert_shopify_lines(lines):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    for line in lines:
        cursor.execute("""
            INSERT OR REPLACE INTO shopify_missing_erp_lines (
                order_name, order_id, created_at, fdm4_order_number,
                fulfillment_order_id, assigned_location,
                line_item_id, line_item_name, sku,
                ordered_quantity, quantity_assigned
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            line["Order Name"],
            line["Order ID"],
            line["Created At"],
            line["FDM4 Order Number"],
            line["Fulfillment Order ID"],
            line["Assigned Location"],
            line["Line Item ID"],
            line["Line Item Name"],
            line["SKU"],
            line["Ordered Quantity"],
            line["Quantity Assigned to Fulfillment"]
        ))

    conn.commit()
    conn.close()

# --- Main execution ---
def main():
    setup_storage_table()
    order_names = get_missing_erp_order_names()
    print(f"[→] Found {len(order_names)} orders missing in ERP to fetch from Shopify.")

    for order_name in order_names:
        print(f"\n[🔎] Querying Shopify for order: {order_name}")

        data = query_order_by_name(order_name)
        if not data:
            print(f"[!] No data returned for {order_name}")
            continue

        if "errors" in data:
            print("GraphQL errors:", data["errors"])
            continue

        orders = data["data"]["orders"]["edges"]
        if not orders:
            print("[!] No orders found on Shopify for this name.")
            continue

        order = orders[0]["node"]

        # 💥 Fixed metafield logic
        meta = order.get("metafield")
        fdm4_order_number = meta.get("value") if meta else None

        gid_order = order["id"].split("/")[-1]
        batch_lines = []

        for fo_edge in order["fulfillmentOrders"]["edges"]:
            fo = fo_edge["node"]
            fo_id = fo["id"].split("/")[-1]
            location = fo["assignedLocation"]
            for li_edge in fo["lineItems"]["edges"]:
                li = li_edge["node"]

                if li["remainingQuantity"] == 0:
                    continue

                line_item = li["lineItem"]
                line_item_id = line_item["id"].split("/")[-1]

                loc_name = location["name"] if location else "N/A"
                if loc_name and len(loc_name) >= 3 and loc_name[:3].isdigit():
                    loc_name = f"AYS{loc_name[:3]}"

                batch_lines.append({
                    "Order Name": order["name"],
                    "Order ID": gid_order,
                    "Created At": order["createdAt"],
                    "FDM4 Order Number": fdm4_order_number,
                    "Fulfillment Order ID": fo_id,
                    "Assigned Location": loc_name,
                    "Line Item ID": line_item_id,
                    "Line Item Name": line_item["name"],
                    "SKU": line_item["sku"],
                    "Ordered Quantity": line_item["quantity"],
                    "Quantity Assigned to Fulfillment": li["remainingQuantity"],
                })

        if batch_lines:
            insert_shopify_lines(batch_lines)
            print(f"[✓] Stored {len(batch_lines)} lines for order {order_name}")
        else:
            print("[!] No shippable line items found to store.")

    print("\n[✅] Done fetching and storing missing ERP orders from Shopify.")

if __name__ == "__main__":
    main()
