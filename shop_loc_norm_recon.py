import sqlite3

DB_PATH = "/home/pscadmin/recon/recon_recon.db"

def update_shop_location_reconciled():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Drop the table if it exists
    cursor.execute("DROP TABLE IF EXISTS shop_location_reconciled")
    print("[🗑️] Dropped existing shop_location_reconciled table.")

    # Create the table using JOINed data from shop_location_normalized + merged_data
    cursor.execute("""
        CREATE TABLE shop_location_reconciled AS
        SELECT
            shop.*,
            merged.match_oms,
            merged.match_erp,
            merged.erp_WarehouseStatus,
            merged.oms_fulfillmentId,

            -- 🧠 Recon State Logic
            CASE
                -- ✅ Good Preorder case
                WHEN merged.match_oms = 1
                 AND merged.match_erp = 1
                 AND merged.erp_WarehouseStatus = 'Backorder'
                 AND (merged.oms_fulfillmentId IS NULL OR TRIM(merged.oms_fulfillmentId) = '')
                 AND shop.erp_preOrder = 'preor'
                THEN 'Good Preorder'

               WHEN merged.match_oms = 1
                AND merged.match_erp = 1
                AND merged.oms_fulfillmentId IS NOT NULL
                AND merged.oms_fulfillmentId != ''
                AND shop.erp_preOrder = 'preor'
                AND merged.erp_WarehouseStatus = 'Backorder'
               THEN 'Preorder Released Backorder'

                WHEN merged.match_oms = 1
	         AND merged.match_erp = 1
	         AND merged.erp_WarehouseStatus = 'Backorder'
	         AND (shop.erp_preOrder IS NULL OR TRIM(shop.erp_preOrder) = '')
	        THEN 'Cancel or Reroute'

                WHEN merged.erp_WarehouseStatus = 'Ready'
                THEN 'Instore Pickup'

                WHEN (merged.erp_WarehouseStatus IS NULL or TRIM(merged.erp_WarehouseStatus) = '')
                 AND merged.match_erp = 1
                THEN 'Run OE Finish'

                WHEN merged.erp_WarehouseStatus = 'Manifested'
                 AND merged.match_erp = 1
                 AND merged.match_oms = 1
                 AND merged.shop_omsLocation = 'AS'
                THEN 'XB Recon Required'

               WHEN (merged.erp_WarehouseStatus IS NULL or TRIM(merged.erp_WarehouseStatus) = '')
                AND merged.match_erp = 0
                AND merged.match_oms = 0 
                AND (shop.erp_preOrder IS NULL OR TRIM(shop.erp_preOrder) = '')
                AND shop.shop_omsLocation = 'Alo Distribution Centers'
               THEN 'No OMS Fulfillment' 

               WHEN merged.match_erp = 0
                AND merged.match_oms = 1
               THEN 'ERP Issue Requires Invest.'

               ELSE ''
            END AS recon_state
        FROM shop_location_normalized shop
        INNER JOIN merged_data merged
          ON shop.shop_order_name = merged.shop_order_name
         AND shop.shop_sku = merged.shop_sku
    """)
    
    conn.commit()
    conn.close()
    print("[✅] shop_location_reconciled table created with match_oms, match_erp, and recon_state.")

if __name__ == "__main__":
    update_shop_location_reconciled()

