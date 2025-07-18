import os
import requests
import sqlite3
from dotenv import load_dotenv

# === Load .env variables ===
load_dotenv()

# === CONFIGURATION ===
STATES = ["CREATED", "ALLOCATED", "RELEASED", "PICKED", "PARTIALLY SHIPPED"]
DEBUG = False

CLIENT_ID = os.getenv("JDA_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("JDA_CLIENT_SECRET", "")
SCOPE = os.getenv("JDA_SCOPE", "https://blueyonderus.onmicrosoft.com/69adb04d-658f-4b86-a659-67fe0f23bd1f/.default")

TOKEN_URL = "https://blueyonderus.b2clogin.com/blueyonderus.onmicrosoft.com/B2C_1A_ClientCredential/oauth2/v2.0/token?realmId=90b51f17-14fa-4a8b-a62f-82f00f709912"
JDA_URL = "https://api.jdadelivers.com/cov/retrieval/v1/customerOrders/elastic-fields"

DB_NAME = "recon_recon.db"

# === DB Setup ===
def setup_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS oms_open_lines")
    cursor.execute("""
        CREATE TABLE oms_open_lines (
            oms_state TEXT,
            oms_order_id TEXT,
            oms_orderName TEXT,
            oms_productID TEXT,
            oms_fulfillmentId TEXT,
            oms_customerOrderRelease_id TEXT,
            oms_location TEXT,
            oms_creationDate TEXT,
            oms_updateTime TEXT,
            oms_lineStatus TEXT,
            is_gwp_line INTEGER DEFAULT 0
        )
    """)
    conn.commit()
    conn.close()
    print("[✅] Recreated oms_open_lines table with oms_ prefixes.")

def insert_jda_line(state, order_id, order_name, product_id, fulfillment_id, release_id, location, creation_date, update_time, line_status):
    is_gwp_line = 1 if str(product_id).startswith("ALOGWP") or str(product_id).startswith("LOYALTY") else 0

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO oms_open_lines (
            oms_state, oms_order_id, oms_orderName, oms_productID,
            oms_fulfillmentId, oms_customerOrderRelease_id,
            oms_location, oms_creationDate, oms_updateTime,
            oms_lineStatus, is_gwp_line
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        state,
        order_id,
        order_name,
        product_id,
        fulfillment_id,
        release_id,
        location,
        creation_date,
        update_time,
        line_status,
        is_gwp_line
    ))
    conn.commit()
    conn.close()

# === Token Retrieval ===
def get_access_token(client_id, client_secret, scope):
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": scope
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    print("🔐 Requesting access token...")
    response = requests.post(TOKEN_URL, data=payload, headers=headers, timeout=30)
    response.raise_for_status()
    token_data = response.json()
    print("✅ Access token acquired.")
    return token_data["access_token"]

# === JDA Fetch ===
def fetch_jda_orders(state, headers, offset=0, limit=100):
    params = {
        "sortFields": "creationDate:desc:date",
        "query": f"orgId=ALO;state={state};",
        "offset": str(offset),
        "limit": str(limit)
    }

    print(f"\n[→] Fetching state: {state} | Offset: {offset} | Limit: {limit}")
    resp = requests.get(JDA_URL, headers=headers, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()

# === Parse and Insert ===
def parse_and_insert(data, state):
    orders = data.get("data", [])
    print(f"[✓] Retrieved {len(orders)} orders for state: {state}")

    for order in orders:
        order_id = order.get("orderId")
        order_name = order.get("customFields", {}).get("orderName")
        creation_date = order.get("creationDate")
        update_time = order.get("updateTime")

        for line in order.get("customerOrderLines", []):
            product_id = line.get("productId")
            line_status = line.get("lineStatus", "UNKNOWN")
            releases = line.get("customerOrderRelease", [])
            if not releases:
                continue

            for release in releases:
                location_id = release.get("locationId")

                if not location_id:
                    location_norm = "UNKNOWN"
                elif location_id == "NETWORK":
                    location_norm = "NETWORK"
                elif location_id.startswith("10"):
                    location_norm = f"AYS{location_id[2:]}"
                else:
                    location_norm = location_id

                fulfillment_id = release.get("fulfillmentId")
                release_id = release.get("customerOrderRelease_id")

                insert_jda_line(
                    state,
                    order_id,
                    order_name,
                    product_id,
                    fulfillment_id,
                    release_id,
                    location_norm,
                    creation_date,
                    update_time,
                    line_status
                )

# === Main ===
def main():
    if not CLIENT_ID or not CLIENT_SECRET:
        print("❌ Error: CLIENT_ID and CLIENT_SECRET must be set in .env file.")
        return

    setup_db()

    access_token = get_access_token(CLIENT_ID, CLIENT_SECRET, SCOPE)
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    for state in STATES:
        if DEBUG:
            data = fetch_jda_orders(state, headers, offset=0, limit=100)
            parse_and_insert(data, state)
        else:
            offset = 0
            page_limit = 1000
            while True:
                data = fetch_jda_orders(state, headers, offset=offset, limit=page_limit)
                parse_and_insert(data, state)

                count = data.get("count", 0)
                if count < page_limit:
                    break
                offset += page_limit

    print("\n[✅] All done. Check oms_open_lines table.")

if __name__ == "__main__":
    main()
