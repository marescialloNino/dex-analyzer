# src/common/paths.py
from dotenv import load_dotenv
import os

load_dotenv()

# Root directory from .env
ROOT_DIR = os.getenv("ROOT_DIR", os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Define common paths
DATA_DIR = os.path.join(ROOT_DIR, 'data')
NOTEBOOKS_DIR = os.path.join(ROOT_DIR, 'notebooks')
POOLS_CSV_DIR = ROOT_DIR  # Assuming pools CSV are saved in root, as per notebook

def get_latest_pools_csv():
    import glob
    csv_files = glob.glob(os.path.join(POOLS_CSV_DIR, 'pools_multi_*.csv'))
    if csv_files:
        return max(csv_files, key=os.path.getctime)
    return None

def delete_old_pools_csv(latest_file):
    import glob
    csv_files = glob.glob(os.path.join(POOLS_CSV_DIR, 'pools_multi_*.csv'))
    for file in csv_files:
        if file != latest_file:
            os.remove(file)
            print(f"Deleted old CSV: {file}")