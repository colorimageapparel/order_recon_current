import os
import pandas as pd
import paramiko
import sqlite3
from dotenv import load_dotenv
from scp import SCPClient

# --- CONFIGURATION ---
load_dotenv()

ERP_HOST = "cidb1"
ERP_PORT = 22
ERP_USER = os.environ.get("ERP_USER")
ERP_PASS = os.environ.get("ERP_PASS")
ERP_PROGRAM = "erp_open_order_lines.p"
ERP_PROGRAM_DIR = "adb"
ERP_REMOTE_DIR = "/u/live/code/"
ERP_OUTPUT_CSV_PATH = "/home/cutsey/andrewb/python/local/shopify/reports/erp_open_order_lines.csv"
LOCAL_OUTPUT_CSV = "erp_open_order_lines.csv"

LOCAL_DB_PATH = r"C:\\Users\\andrew.beattie\\AppData\\Local\\Programs\\Python\\Python313\\Lib\\Projects_2\\recon_recon\\recon_recon.db"

# === Run ERP .p program and fetch CSV ===
def run_erp_and_fetch_csv():
    print("[→] Connecting to ERP server via SSH...")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ERP_HOST, port=ERP_PORT, username=ERP_USER, password=ERP_PASS)
    scp = SCPClient(ssh.get_transport())

    erp_cmd = f"cd {ERP_REMOTE_DIR} && /usr/dlc117/bin/mpro -b -pf connect.pf -p {ERP_PROGRAM_DIR}/{ERP_PROGRAM}"
    print("[→] Executing ERP .p script remotely...")
    stdin, stdout, stderr = ssh.exec_command(erp_cmd)

    for line in stdout:
        print("[ERP]", line.strip())
    for err in stderr:
        print("[!] ERP Error:", err.strip())

    print(f"[→] Fetching ERP output CSV: {ERP_OUTPUT_CSV_PATH}")
    scp.get(ERP_OUTPUT_CSV_PATH, LOCAL_OUTPUT_CSV)

    scp.close()
    ssh.close()
    print(f"[✓] ERP output CSV downloaded: {LOCAL_OUTPUT_CSV}")

# === Load ERP CSV into local DB with prefix columns ===
def load_erp_csv_to_db():
    df = pd.read_csv(LOCAL_OUTPUT_CSV)

    # Add prefix to all columns
    df = df.rename(columns={col: f"erp_{col}" for col in df.columns})

    conn = sqlite3.connect(LOCAL_DB_PATH)
    cursor = conn.cursor()

    # Drop table if exists to refresh schema
    cursor.execute("DROP TABLE IF EXISTS erp_open_lines")
    conn.commit()
    print("[🗑️] Dropped existing erp_open_lines table to rebuild schema.")

    # Dynamically create columns based on prefixed CSV columns
    columns = df.columns
    col_types = []
    for col in columns:
        col_types.append(f'"{col}" TEXT')
    col_types.append('"is_gwp_line" INTEGER DEFAULT 0')  # Explicitly add GWP flag

    create_table_sql = f"CREATE TABLE erp_open_lines ({', '.join(col_types)});"
    cursor.execute(create_table_sql)
    conn.commit()
    print("[✅] Recreated erp_open_lines table with new columns (prefixed).")

    # Add is_gwp_line column data
    df["is_gwp_line"] = df["erp_ItemNumber"].apply(lambda x: 1 if str(x).startswith("ALOGWP") or str(x).startswith("LOYALTY") else 0)

    # Load data
    df.to_sql("erp_open_lines", conn, if_exists='append', index=False)
    conn.commit()
    conn.close()
    print(f"[✓] ERP data loaded into table: erp_open_lines in {LOCAL_DB_PATH}")

# === Main execution ===
def main():
    run_erp_and_fetch_csv()
    load_erp_csv_to_db()
    print("[✅] Full ERP workflow completed successfully.")

if __name__ == "__main__":
    main()
