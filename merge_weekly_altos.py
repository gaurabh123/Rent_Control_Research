import pandas as pd
import re
from pathlib import Path

BASE_DIR = Path("..")  
OUTPUT_FILE = "altos_merged_all.csv"

def extract_date_from_filename(fname: str):
    m = re.search(r'(\d{4}-\d{2}-\d{2})', fname)
    if m:
        return pd.to_datetime(m.group(1), errors="coerce")
    
    m = re.search(r'(\d{8})', fname)
    if m:
        return pd.to_datetime(m.group(1), format="%Y%m%d", errors="coerce")
    
    return pd.NaT

def read_all_weeklies(base_dir: Path, date_col: str = "date") -> pd.DataFrame:
    csvs = sorted(base_dir.rglob("*.csv"))
    frames = []
    
    print(f"Found {len(csvs)} files. Beginning ingestion...")

    for p in csvs:
        df = pd.read_csv(p, low_memory=False)

        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        else:
            dt = extract_date_from_filename(p.name)
            df[date_col] = dt
        df["src_file"] = p.name
        frames.append(df)

    out = pd.concat(frames, ignore_index=True)
    
    out = out.dropna(subset=[date_col])
    return out


altos_all = read_all_weeklies(BASE_DIR, date_col="date")

print("Merged shape:", altos_all.shape)
print("Date range:", altos_all["date"].min().date(), "→", altos_all["date"].max().date())
altos_all.head()

altos_all.to_csv("altos_merged_all.csv", index=False)
