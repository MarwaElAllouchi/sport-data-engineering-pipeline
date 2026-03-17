import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import glob
import json
import os
import pandas as pd
from utils.utils_logger import setup_logger
from  config import DUCKDB_FILE

EXPORT_DIR = "data/powerbi/"
EXPORT_DIR_file_db= DUCKDB_FILE 

MIN_TOTAL_ROWS = 1
logger = setup_logger("check_volumetrie")
print(f"ddddddddddddddduck db {EXPORT_DIR_file_db}")

csv_files = glob.glob(os.path.join(EXPORT_DIR, "powerbi_*.csv"))
duckdb_files = glob.glob(os.path.join(EXPORT_DIR_file_db, "*.duckdb"))

if not csv_files:
    raise ValueError("Aucun fichier CSV Power BI n'a été généré dans exports/")

if not duckdb_files:
    raise ValueError("Aucun fichier DuckDB n'a été généré dans exports/")

total_rows = 0
file_details = []

for file_path in csv_files:
    df = pd.read_csv(file_path)
    row_count = len(df)
    total_rows += row_count
    file_details.append({
        "file_name": os.path.basename(file_path),
        "row_count": row_count
    })

report = {
    "csv_file_count": len(csv_files),
    "duckdb_file_count": len(duckdb_files),
    "total_rows": total_rows,
    "files": file_details,
    "status": "SUCCESS" if total_rows >= MIN_TOTAL_ROWS else "FAILED"
}

with open("volumetry_report.json", "w", encoding="utf-8") as f:
    json.dump(report, f, indent=2, ensure_ascii=False)

logger.info("=== RAPPORT DE VOLUMÉTRIE ===")
logger.info(json.dumps(report, indent=2, ensure_ascii=False))

if total_rows < MIN_TOTAL_ROWS:
    raise ValueError(
        f"Volumétrie anormale : {total_rows} lignes exportées, seuil minimal attendu = {MIN_TOTAL_ROWS}"
    )

logger.info("✅ Contrôle volumétrique validé.")