import requests
import json
import concurrent.futures
import re
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import datetime

# --- CONFIGURATION ---
BASE_URL = "https://www150.statcan.gc.ca/t1/cimt/rest/getReport/"
OUTPUT_DIR = "src/data"
YEARS = [2008, 2009, 2010]
MONTHS = range(1, 13)
FLOWS = [0, 1] # 0 = Export, 1 = Import

# --- DATA LOADING ---
def load_json(path):
    try:
        with open(path, 'r', encoding='utf-8') as f: return json.load(f)
    except Exception as e:
        print(f"Warning: Failed to load {path}: {e}")
        return []

print("Loading reference data...")
try:
    CHAPTER_MAP = { item['HS']: item['EN'] for item in load_json('data/reference/chapters.json') }
    PROVINCES = {p['id']: p['en'] for p in load_json('data/reference/provinces.json')}
except:
    print("Error loading reference data. Ensure data/reference/ exists.")
    CHAPTER_MAP = {"01": "Live animals"} # Fallback
    PROVINCES = {1: "Newfoundland and Labrador"}

# --- NETWORKING ---
session = requests.Session()
retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
session.mount('https://', HTTPAdapter(max_retries=retries))

def fetch_data(year, month, chapter, prov_id, flow_id):
    # P1: ProvID
    # P4: Chapter
    # P5: Flow (0=Exp, 1=Imp)
    # P6: 150000 (Detailed Partner)
    
    date_str = f"{year}-{month:02d}-01"
    url = f"{BASE_URL}({prov_id})/0/100/{chapter}/{flow_id}/150000/0/0/{date_str}/{date_str}"
    
    try:
        r = session.get(url, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return None

def fetch_task(year, month, chapter, prov_id, flow_id):
    # Flow 0 = Export -> data/raw/
    # Flow 1 = Import -> data/raw_imports/
    if flow_id == 0:
        raw_dir = f"data/raw/{year}/{chapter}"
    else:
        raw_dir = f"data/raw_imports/{year}/{chapter}"
    os.makedirs(raw_dir, exist_ok=True)
    
    filename = f"{month:02d}_{prov_id}.json"
    filepath = os.path.join(raw_dir, filename)
    
    # Skip if exists and valid
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = json.load(f)
                return True # Success (Cached)
        except:
            pass # Invalid, re-fetch

    data = fetch_data(year, month, chapter, prov_id, flow_id)
    
    if data:
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f)
            return True # Success (New)
        except Exception as e:
            print(f"Warning: Failed to save {filepath}: {e}")
            
    return False # Failed

def main():
    targets = list(PROVINCES.keys()) + [0]
    target_chapters = sorted(list(CHAPTER_MAP.keys()))
    
    total_tasks = len(targets) * len(YEARS) * len(MONTHS) * len(target_chapters) * len(FLOWS)
    
    print(f"Starting UNIFIED extraction for {total_tasks} tasks (Flows: {FLOWS})...")
    print(f"Years: {YEARS}")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for flow in FLOWS:
            for year in YEARS:
                for month in MONTHS:
                    for chapter in target_chapters:
                        for pid in targets:
                            futures.append(executor.submit(fetch_task, year, month, chapter, pid, flow))
        
        count = 0
        completed = 0
        for future in concurrent.futures.as_completed(futures):
            count += 1
            if future.result():
                completed += 1
                
            if count % 100 == 0:
                print(f"Progress: {count}/{total_tasks} ({completed} success)...", flush=True)
            
    print("Done. All requested data extracted.")

if __name__ == "__main__":
    main()
