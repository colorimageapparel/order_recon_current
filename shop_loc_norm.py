import sqlite3

DB_PATH = "recon_recon.db"

def setup_normalized_table():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS shop_location_normalized")

    cursor.execute("""
        CREATE TABLE shop_location_normalized AS
        SELECT
            shop.shop_order_name,
            shop.shop_order_id,
            shop.shop_created_at,
            shop.shop_fdm4_order_number,
            shop.shop_fulfillment_order_id,
            shop.shop_assigned_location,
            shop.shop_line_item_id,
            shop.shop_line_item_name,
            shop.shop_sku,
            shop.shop_ordered_quantity,
            shop.shop_quantity_assigned,
            shop.shop_actual_location,
            shop.is_gwp_line,
            erp.erp_preOrder,
            CASE
                WHEN shop.shop_assigned_location = 'Alo Distribution Centers' AND oms.oms_location IS NOT NULL
                    THEN oms.oms_location
                ELSE shop.shop_assigned_location
            END AS shop_omsLocation
        FROM shop_open_lines shop
        LEFT JOIN oms_open_lines oms
            ON shop.shop_order_name = oms.oms_orderName
           AND shop.shop_sku = oms.oms_productID
        LEFT JOIN erp_open_lines erp
            ON erp.erp_CustOrderNum = shop.shop_order_name
            AND erp.erp_ItemNumber = shop.shop_sku
    """)

    conn.commit()
    conn.close()
    print("[✅] shop_location_normalized table created and populated with `is_preorder` included.")

if __name__ == "__main__":
    setup_normalized_table()

