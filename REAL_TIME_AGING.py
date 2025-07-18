#!/usr/bin/env python3

import subprocess
import json
import datetime
import traceback
import os

# === CONFIG ===
PYTHON_PATH = "python3"  # Update if needed
STATUS_FILE = "/home/pscadmin/recon/process_status.json"
os.chdir("/home/pscadmin/recon")
scripts = [
    "shop_open_released.py",
    "oms_open_lines.py",
    "shop_loc_norm.py",
    "merged_open_lines.py",
    "shop_loc_norm_recon.py",
    "erp_missing_aging.py",
]

def write_status(status, start_time=None, end_time=None, failed_script=None, error=None):
    status_data = {
        "status": status,
        "start_time": start_time,
        "end_time": end_time,
        "failed_script": failed_script,
        "error": error,
    }
    with open(STATUS_FILE, "w") as f:
        json.dump(status_data, f, indent=2)

def main():
    start_ts = datetime.datetime.now().isoformat()
    write_status("running", start_time=start_ts)

    try:
        for script in scripts:
            print(f"\n🚀 Running: {script} ...")
            result = subprocess.run([PYTHON_PATH, script])
            if result.returncode != 0:
                print(f"❌ Error running {script}. Stopping.")
                raise RuntimeError(f"{script} failed with return code {result.returncode}")

            print(f"✅ Completed: {script}")

        end_ts = datetime.datetime.now().isoformat()
        write_status("complete", start_time=start_ts, end_time=end_ts)
        print(f"\n[✅] All scripts completed at {end_ts}.")

    except Exception as e:
        end_ts = datetime.datetime.now().isoformat()
        tb = traceback.format_exc()
        write_status("failed", start_time=start_ts, end_time=end_ts, failed_script=script, error=tb)
        print(f"\n[❌] Process failed during {script}:\n{tb}")

if __name__ == "__main__":
    main()
