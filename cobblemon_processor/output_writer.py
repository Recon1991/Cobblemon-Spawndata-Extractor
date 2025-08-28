
import csv
from typing import List, Dict
try:
    from .column_names import column_names, skipped_entries_column_names
except Exception:
    # fallback if running as a module and column_names is adjacent
    import sys, pathlib
    sys.path.append(str(pathlib.Path(__file__).resolve().parent.parent))
    from .column_names import column_names, skipped_entries_column_names

def write_main_csv(rows: List[Dict], filename: str, batch_size: int = 1000):
    with open(filename, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=column_names)
        w.writeheader()
        batch = []
        for r in rows:
            batch.append(r)
            if len(batch) >= batch_size:
                w.writerows(batch); batch = []
        if batch: w.writerows(batch)

def write_skipped_entries_csv(rows: List[Dict], filename: str, batch_size: int = 1000):
    with open(filename, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=skipped_entries_column_names)
        w.writeheader()
        batch = []
        for r in rows:
            batch.append(r)
            if len(batch) >= batch_size:
                w.writerows(batch); batch = []
        if batch: w.writerows(batch)

def write_additions_csv(rows: List[Dict], filename: str, batch_size: int = 1000):
    with open(filename, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=column_names)
        w.writeheader()
        batch = []
        for r in rows:
            batch.append(r)
            if len(batch) >= batch_size:
                w.writerows(batch); batch = []
        if batch: w.writerows(batch)
