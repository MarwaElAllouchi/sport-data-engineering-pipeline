import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import io
import duckdb
import pandas as pd
import boto3
from utils.utils_logger import setup_logger 
from utils.utils_cleaning import ensure_parent_dir

logger = setup_logger("load_to_duckdb")

from config import (
    BUCKET,
    AWS_REGION,
    SUMMARY_KEY,
    FINANCIALS_KEY,
    DUCKDB_FILE,
    DUCKDB_SUMMARY_TABLE,
    DUCKDB_FINANCIALS_TABLE
)

s3 = boto3.client("s3", region_name=AWS_REGION)

# 1) Lire les datasets depuis S3
summary_obj = s3.get_object(Bucket=BUCKET, Key=SUMMARY_KEY)
summary_df = pd.read_csv(io.BytesIO(summary_obj["Body"].read()))

financials_obj = s3.get_object(Bucket=BUCKET, Key=FINANCIALS_KEY)
financials_df = pd.read_csv(io.BytesIO(financials_obj["Body"].read()))

# --- CORRECTION DES TYPES ---
# On force les colonnes de type 'object' (souvent des str) en type string propre pour DuckDB
summary_df = summary_df.convert_dtypes()
financials_df = financials_df.convert_dtypes()
# ----------------------------

# 2) Connexion DuckDB
ensure_parent_dir(DUCKDB_FILE)
con = duckdb.connect(DUCKDB_FILE)

# 3) Recréer les tables
con.execute(f"DROP TABLE IF EXISTS {DUCKDB_SUMMARY_TABLE}")
con.execute(f"DROP TABLE IF EXISTS {DUCKDB_FINANCIALS_TABLE}")

# Utilisation directe de DuckDB pour ingérer le DataFrame (plus stable que .register)
con.execute(f"CREATE TABLE {DUCKDB_SUMMARY_TABLE} AS SELECT * FROM summary_df")
con.execute(f"CREATE TABLE {DUCKDB_FINANCIALS_TABLE} AS SELECT * FROM financials_df")

# 4) Vérification rapide
summary_count = con.execute(f"SELECT COUNT(*) FROM {DUCKDB_SUMMARY_TABLE}").fetchone()[0]
financials_count = con.execute(f"SELECT COUNT(*) FROM {DUCKDB_FINANCIALS_TABLE}").fetchone()[0]

logger.info("✅ Chargement DuckDB terminé")
logger.info(f"Base DuckDB : {DUCKDB_FILE}")
logger.info(f"Table employee_sport_summary : {summary_count} lignes")
logger.info(f"Table employee_sport_financials : {financials_count} lignes")

con.close()