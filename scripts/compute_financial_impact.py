#compute_financial_impact.py
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import io
import unicodedata
import pandas as pd
import boto3
from utils.utils_cleaning import ensure_parent_dir 
from utils.utils_logger import setup_logger
from datetime import datetime

logger = setup_logger("Calcul_prime_sportive")
from config import (
    BUCKET,
    AWS_REGION,
    SUMMARY_KEY,
    COMMUTE_KEY,
    FINANCIALS_KEY,
    LOCAL_FINANCIALS_FILE,
    PRIME_RATE
)

s3 = boto3.client("s3", region_name=AWS_REGION)


def normalize_text(value):
    if pd.isna(value):
        return ""
    value = str(value).strip().lower()
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("utf-8")
    return value


# 1️⃣ Lire les datasets
summary_obj = s3.get_object(Bucket=BUCKET, Key=SUMMARY_KEY)
summary_df = pd.read_csv(io.BytesIO(summary_obj["Body"].read()))

commute_obj = s3.get_object(Bucket=BUCKET, Key=COMMUTE_KEY)
commute_df = pd.read_csv(io.BytesIO(commute_obj["Body"].read()))

# 2️⃣ Harmoniser id
summary_df["id_salarie"] = pd.to_numeric(summary_df["id_salarie"], errors="coerce")
commute_df["id_salarie"] = pd.to_numeric(commute_df["id_salarie"], errors="coerce")

# 3️⃣ Jointure
df = summary_df.merge(
    commute_df[
        [
            "id_salarie",
            "distance_km",
            "distance_threshold_km",
            "trajet_coherent",
            "anomalie_declaration",
            "geocoding_status"
        ]
    ],
    on="id_salarie",
    how="left"
)

# 4️⃣ Normaliser moyen de transport
df["moyen_de_deplacement_normalise"] = df["moyen_de_deplacement"].apply(normalize_text)

logger.info("Valeurs distinctes :")
logger.info(sorted(df["moyen_de_deplacement_normalise"].dropna().unique().tolist()))

sport_transports = {
    "marche/running",
    "velo/trottinette/autres"
}

# 5️⃣ Eligibilité finale
def compute_prime_eligibility(row):

    mode_ok = row["moyen_de_deplacement_normalise"] in sport_transports
    trajet_ok = row["trajet_coherent"] == "oui"

    return "oui" if mode_ok and trajet_ok else "non"


df["eligible_prime_sport"] = df.apply(compute_prime_eligibility, axis=1)

# 6️⃣ Calcul prime
df["salaire_brut"] = pd.to_numeric(df["salaire_brut"], errors="coerce").fillna(0)

df["prime_sport"] = df.apply(
    lambda row: row["salaire_brut"] * PRIME_RATE if row["eligible_prime_sport"] == "oui" else 0,
    axis=1
)
execution_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
df["execution_date"] = execution_date

# 7️⃣ Sauvegarde
ensure_parent_dir(LOCAL_FINANCIALS_FILE)

df.to_csv(LOCAL_FINANCIALS_FILE, index=False)

s3.upload_file(
    LOCAL_FINANCIALS_FILE,
    BUCKET,
    FINANCIALS_KEY
)

# 8️⃣ Stats
nb_eligibles = (df["eligible_prime_sport"] == "oui").sum()
total_prime = df["prime_sport"].sum()

logger.info("✅ Calcul prime sportive terminé")
logger.info("Salariés éligibles : %s", nb_eligibles)
logger.info("Impact financier total : %s €", round(total_prime, 2))
logger.info("Anomalies trajet : %s", (df["anomalie_declaration"] == "oui").sum())
logger.info("Adresses non trouvées : %s", (df["geocoding_status"] != "ok").sum())
logger.info("Fichier S3 : %s", f"s3://{BUCKET}/{FINANCIALS_KEY}")