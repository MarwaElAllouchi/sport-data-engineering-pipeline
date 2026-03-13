import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import io
import pandas as pd
import boto3
from utils.utils_cleaning import ensure_parent_dir,clean_columns, remove_duplicates, standardize_text, split_clean_rejects
from utils.utils_logger import setup_logger
from config import (BUCKET ,AWS_REGION,KEY,RH_KEY,RH_KEY_REJECTS,LOCAL_RH_CLEAN,LOCAL_RH_REJECTS_FILE)
 
logger = setup_logger("Nettoyage_rh")
s3 = boto3.client("s3", region_name=AWS_REGION)

obj = s3.get_object(Bucket=BUCKET, Key=KEY)
file_content = obj["Body"].read()

df = pd.read_excel(io.BytesIO(file_content), engine="openpyxl")

df = clean_columns(df)
df = remove_duplicates(df)

print("Colonnes après nettoyage :", df.columns.tolist())

df["salaire_brut"] = pd.to_numeric(df["salaire_brut"], errors="coerce")

if "moyen_de_deplacement" in df.columns:
    df["moyen_de_deplacement"] = standardize_text(df["moyen_de_deplacement"])

reject_mask = (
    df["id_salarie"].isna()
    | df["salaire_brut"].isna()
    | (df["salaire_brut"] <= 0)
    | df["adresse_du_domicile"].isna()
)

clean, rejects = split_clean_rejects(df, reject_mask)

ensure_parent_dir(LOCAL_RH_CLEAN)
ensure_parent_dir(LOCAL_RH_REJECTS_FILE)

clean.to_csv(LOCAL_RH_CLEAN, index=False)
rejects.to_csv(LOCAL_RH_REJECTS_FILE, index=False)

s3.upload_file(RH_KEY, BUCKET, RH_KEY)
s3.upload_file(RH_KEY_REJECTS, BUCKET, RH_KEY_REJECTS)

logger.info("✅ RH nettoyé")
logger.info("Lignes propres :", len(clean))
logger.info("Lignes rejetées :", len(rejects))