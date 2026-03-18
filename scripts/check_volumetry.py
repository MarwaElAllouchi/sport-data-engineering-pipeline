#check_volumetry.py
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import glob
import json
import pandas as pd
from utils.utils_logger import setup_logger
from config import DUCKDB_FILE

EXPORT_DIR = "data/powerbi/"
MIN_TOTAL_ROWS = 1

logger = setup_logger("check_volumetrie")
logger.info(f"Chemin DuckDB attendu : {DUCKDB_FILE}")

csv_files = glob.glob(os.path.join(EXPORT_DIR, "powerbi_*.csv"))

if not csv_files:
    raise ValueError(f"Aucun fichier CSV Power BI n'a été généré dans {EXPORT_DIR}")

if not os.path.exists(DUCKDB_FILE):
    raise ValueError(f"Aucun fichier DuckDB n'a été généré : {DUCKDB_FILE}")

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
    "duckdb_file_count": 1,
    "duckdb_file": DUCKDB_FILE,
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