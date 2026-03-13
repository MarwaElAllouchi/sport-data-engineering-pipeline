import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import io
import pandas as pd
import boto3

from utils.utils_logger import setup_logger

logger = setup_logger("test_financials")

BUCKET = "sport-data-lake"
FINANCIALS_KEY = "curated/employee_sport_financials.csv"

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


logger.info("=== TEST FINANCIALS ===")

df = read_csv_from_s3(BUCKET, FINANCIALS_KEY)

df["prime_sport"] = pd.to_numeric(df["prime_sport"], errors="coerce")

log_result(
    "prime_sport >= 0",
    (df["prime_sport"] >= 0).all()
)
prime_check = df.apply(
    lambda row: (
        (row["eligible_prime_sport"] == "non" and row["prime_sport"] == 0)
        or (row["eligible_prime_sport"] == "oui" and row["prime_sport"] > 0)
    ),
    axis=1
)

log_result(
    "coherence eligibilite prime",
    prime_check.all()
)

if "trajet_coherent" in df.columns:
    eligible_prime_rows = df[df["eligible_prime_sport"] == "oui"]

    log_result(
        "eligible prime => trajet_coherent = oui",
        (eligible_prime_rows["trajet_coherent"] == "oui").all()
    )
logger.info("=== FIN TEST FINANCIALS ===")