#test_summary.py
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import io
import pandas as pd
import boto3

from utils.utils_logger import setup_logger

logger = setup_logger("test_summary")

BUCKET = "sport-data-lake"
SUMMARY_KEY = "curated/employee_sport_summary.csv"

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


logger.info("=== TEST SUMMARY ===")

summary_df = read_csv_from_s3(BUCKET, SUMMARY_KEY)

summary_df["nb_activites_12_mois"] = pd.to_numeric(summary_df["nb_activites_12_mois"], errors="coerce")
summary_df["jours_bien_etre"] = pd.to_numeric(summary_df["jours_bien_etre"], errors="coerce")

log_result(
    "nb_activites_12_mois >= 0",
    (summary_df["nb_activites_12_mois"] >= 0).all()
)

log_result(
    "jours_bien_etre dans {0,5}",
    summary_df["jours_bien_etre"].isin([0, 5]).all()
)
eligible_check = summary_df.apply(
    lambda row: (
        (row["eligible_bien_etre"] == "oui" and row["nb_activites_12_mois"] >= 15)
        or (row["eligible_bien_etre"] == "non" and row["nb_activites_12_mois"] < 15)
    ),
    axis=1
)

log_result(
    "coherence eligibilite bien-etre",
    eligible_check.all()
)
logger.info("=== FIN TEST SUMMARY ===")