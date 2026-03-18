#test_activities.py
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import io
import pandas as pd
import boto3

from utils.utils_logger import setup_logger

logger = setup_logger("test_activities")

BUCKET = "sport-data-lake"
ACTIVITIES_KEY = "staging/sport/sport_activities_clean.csv"
RH_KEY = "staging/rh/rh_clean.csv"

s3 = boto3.client("s3", region_name="eu-west-3")


def read_csv_from_s3(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))


def log_result(test_name, passed):

    if passed:
        logger.info(f"PASS - {test_name}")
    else:
        logger.error(f"FAIL - {test_name}")
        raise Exception(f"Test échoué : {test_name}")


logger.info("=== TEST ACTIVITES ===")

activities_df = read_csv_from_s3(BUCKET, ACTIVITIES_KEY)
rh_df = read_csv_from_s3(BUCKET, RH_KEY)

activities_df["date_debut"] = pd.to_datetime(activities_df["date_debut"], errors="coerce")
activities_df["date_fin"] = pd.to_datetime(activities_df["date_fin"], errors="coerce")

activities_df["distance_m"] = pd.to_numeric(activities_df["distance_m"], errors="coerce")
activities_df["temps_ecoule_s"] = pd.to_numeric(activities_df["temps_ecoule_s"], errors="coerce")

valid_employee_ids = set(pd.to_numeric(rh_df["id_salarie"], errors="coerce").dropna().astype(int))

log_result(
    "date_fin > date_debut",
    (activities_df["date_fin"] > activities_df["date_debut"]).all()
)

log_result(
    "distance_m >= 0",
    (activities_df["distance_m"] >= 0).all()
)

log_result(
    "temps_ecoule_s >= 0",
    (activities_df["temps_ecoule_s"] >= 0).all()
)

log_result(
    "id_salarie existe dans RH",
    activities_df["id_salarie"].dropna().astype(int).isin(valid_employee_ids).all()
)

logger.info("=== FIN TEST ACTIVITES ===")