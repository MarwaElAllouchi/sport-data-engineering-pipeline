#test_commute.py
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import io
import pandas as pd
import boto3

from utils.utils_logger import setup_logger

logger = setup_logger("test_commute")

BUCKET = "sport-data-lake"
COMMUTE_KEY = "curated/employee_commute_validation.csv"
RH_KEY = "staging/rh/rh_clean.csv"

s3 = boto3.client("s3", region_name="eu-west-3")


def read_csv_from_s3(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))


def log_result(test_name, passed, detail=""):
    if passed:
        logger.info(f"PASS - {test_name}")
        if detail:
            logger.info(detail)
    else:
        logger.error(f"FAIL - {test_name}")
        if detail:
            logger.error(detail)
        raise Exception(f"Test échoué : {test_name}")


logger.info("=== TEST COMMUTE ===")

commute_df = read_csv_from_s3(BUCKET, COMMUTE_KEY)
rh_df = read_csv_from_s3(BUCKET, RH_KEY)

commute_df["id_salarie"] = pd.to_numeric(commute_df["id_salarie"], errors="coerce")
commute_df["distance_km"] = pd.to_numeric(commute_df["distance_km"], errors="coerce")
commute_df["distance_threshold_km"] = pd.to_numeric(commute_df["distance_threshold_km"], errors="coerce")

rh_df["id_salarie"] = pd.to_numeric(rh_df["id_salarie"], errors="coerce")
valid_employee_ids = set(rh_df["id_salarie"].dropna().astype(int).tolist())

# 1) id_salarie doit exister dans RH
log_result(
    "id_salarie existe dans RH",
    commute_df["id_salarie"].dropna().astype(int).isin(valid_employee_ids).all()
)

# 2) distance_km doit être >= 0 quand elle existe
distance_valid = commute_df["distance_km"].dropna()
log_result(
    "distance_km >= 0 si renseignee",
    (distance_valid >= 0).all()
)

# 3) threshold autorisé = 15 ou 25 quand il existe
threshold_valid = commute_df["distance_threshold_km"].dropna()
log_result(
    "distance_threshold_km dans {15,25}",
    threshold_valid.isin([15, 25]).all()
)

# 4) si trajet_coherent = oui alors distance <= seuil
coherent_rows = commute_df[
    (commute_df["trajet_coherent"] == "oui")
    & commute_df["distance_km"].notna()
    & commute_df["distance_threshold_km"].notna()
]
log_result(
    "coherence trajet_coherent = oui",
    (coherent_rows["distance_km"] <= coherent_rows["distance_threshold_km"]).all()
)

# 5) si anomalie_declaration = oui alors distance > seuil
anomaly_rows = commute_df[
    (commute_df["anomalie_declaration"] == "oui")
    & commute_df["distance_km"].notna()
    & commute_df["distance_threshold_km"].notna()
]
log_result(
    "coherence anomalie_declaration = oui",
    (anomaly_rows["distance_km"] > anomaly_rows["distance_threshold_km"]).all() if not anomaly_rows.empty else True,
    "Aucune anomalie détectée" if anomaly_rows.empty else ""
)

# 6) geocoding_status doit être renseigné
not_found_rows = commute_df[commute_df["geocoding_status"] != "ok"]
log_result(
    "geocoding_status renseigne",
    commute_df["geocoding_status"].notna().all(),
    f"Adresses non trouvées : {len(not_found_rows)}"
)

logger.info("=== FIN TEST COMMUTE ===")