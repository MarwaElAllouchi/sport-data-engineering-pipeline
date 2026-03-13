import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import io
import pandas as pd
import boto3
from utils.utils_logger import setup_logger
from utils.utils_cleaning import clean_columns, remove_duplicates, standardize_text, split_clean_rejects
from config import KEY_SPORT,BUCKET , AWS_REGION,LOCAL_SPORT_CLEAN,LOCAL_SPORT_REJECTS_FILE,SPORT_DECLARATIF_KEY,SPORT_KEY_REJECTS
logger = setup_logger("Nettoyage_Sport")

KEY = KEY_SPORT
s3 = boto3.client("s3", region_name=AWS_REGION)

obj = s3.get_object(Bucket=BUCKET, Key=KEY)
file_content = obj["Body"].read()

df = pd.read_excel(io.BytesIO(file_content), engine="openpyxl")

df = clean_columns(df)
df = remove_duplicates(df)

print("Colonnes après nettoyage :", df.columns.tolist())

# normaliser la colonne texte
if "pratique_dun_sport" in df.columns:
    df["pratique_dun_sport"] = standardize_text(df["pratique_dun_sport"])

    df["pratique_dun_sport"] = df["pratique_dun_sport"].replace({
        "yes": "oui",
        "y": "oui",
        "true": "oui",
        "1": "oui",
        "no": "non",
        "n": "non",
        "false": "non",
        "0": "non"
    })

reject_mask = df["id_salarie"].isna()

clean, rejects = split_clean_rejects(df, reject_mask)

clean.to_csv(LOCAL_SPORT_CLEAN, index=False)
rejects.to_csv(LOCAL_SPORT_REJECTS_FILE, index=False)

s3.upload_file(LOCAL_SPORT_CLEAN, BUCKET, SPORT_DECLARATIF_KEY)
s3.upload_file(LOCAL_SPORT_REJECTS_FILE, BUCKET, SPORT_KEY_REJECTS)

print("✅ Sport déclaratif nettoyé")
print("Lignes propres :", len(clean))
print("Lignes rejetées :", len(rejects))