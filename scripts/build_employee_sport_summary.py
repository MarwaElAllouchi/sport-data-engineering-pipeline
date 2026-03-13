
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import io
import pandas as pd
import boto3

from config import (BUCKET, AWS_REGION,RH_KEY,SPORT_DECLARATIF_KEY,ACTIVITIES_CLEAN_KEY,SUMMARY_KEY,LOCAL_SUMMARY_FILE)
from utils.utils_logger import setup_logger

ACTIVITIES_KEY = ACTIVITIES_CLEAN_KEY
OUTPUT_LOCAL = LOCAL_SUMMARY_FILE
OUTPUT_S3 = SUMMARY_KEY

s3 = boto3.client("s3", region_name=AWS_REGION)
logger = setup_logger("build_employee_sport_summary")
# 1) Lire les fichiers depuis S3
rh_obj = s3.get_object(Bucket=BUCKET, Key=RH_KEY)
rh_df = pd.read_csv(io.BytesIO(rh_obj["Body"].read()))

sport_decl_obj = s3.get_object(Bucket=BUCKET, Key=SPORT_DECLARATIF_KEY)
sport_decl_df = pd.read_csv(io.BytesIO(sport_decl_obj["Body"].read()))

activities_obj = s3.get_object(Bucket=BUCKET, Key=ACTIVITIES_KEY)
activities_df = pd.read_csv(io.BytesIO(activities_obj["Body"].read()))

# 2) Harmoniser id_salarie
rh_df["id_salarie"] = pd.to_numeric(rh_df["id_salarie"], errors="coerce")
sport_decl_df["id_salarie"] = pd.to_numeric(sport_decl_df["id_salarie"], errors="coerce")
activities_df["id_salarie"] = pd.to_numeric(activities_df["id_salarie"], errors="coerce")

# 3) Compter les activités par salarié
activities_count = (
    activities_df
    .groupby("id_salarie", as_index=False)
    .size()
    .rename(columns={"size": "nb_activites_12_mois"})
)

# 4) Jointure RH + sport déclaratif
summary_df = rh_df.merge(
    sport_decl_df,
    on="id_salarie",
    how="left"
)

# 5) Jointure avec nombre d'activités
summary_df = summary_df.merge(
    activities_count,
    on="id_salarie",
    how="left"
)

# 6) Remplir les valeurs manquantes
summary_df["nb_activites_12_mois"] = summary_df["nb_activites_12_mois"].fillna(0).astype(int)

if "pratique_dun_sport" in summary_df.columns:
    summary_df["pratique_dun_sport"] = summary_df["pratique_dun_sport"].fillna("non")

# 7) Calcul éligibilité jours bien-être
summary_df["eligible_bien_etre"] = summary_df["nb_activites_12_mois"].apply(
    lambda x: "oui" if x >= 15 else "non"
)

summary_df["jours_bien_etre"] = summary_df["nb_activites_12_mois"].apply(
    lambda x: 5 if x >= 15 else 0
)

# 8) Sauvegarde locale
summary_df.to_csv(OUTPUT_LOCAL, index=False)

# 9) Upload S3
s3.upload_file(OUTPUT_LOCAL, BUCKET, OUTPUT_S3)

logger.info("✅ Résumé salarié construit")
logger.info("Nombre de salariés : %s", len(summary_df))
logger.info("Salariés éligibles bien-être : %s", (summary_df['eligible_bien_etre'] == 'oui').sum())
logger.info("Total jours bien-être accordés : %s", summary_df["jours_bien_etre"].sum())
logger.info("Fichier S3 : %s", f"s3://{BUCKET}/{OUTPUT_S3}")