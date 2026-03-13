import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import io
import pandas as pd
import boto3

from utils.utils_logger import setup_logger

logger = setup_logger("test_rh")

BUCKET = "sport-data-lake"
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


logger.info("=== TEST RH ===")

rh_df = read_csv_from_s3(BUCKET, RH_KEY)

rh_df["id_salarie"] = pd.to_numeric(rh_df["id_salarie"], errors="coerce")

log_result(
    "id_salarie non vide",
    rh_df["id_salarie"].notna().all()
)

log_result(
    "salaire_brut > 0",
    (pd.to_numeric(rh_df["salaire_brut"], errors="coerce") > 0).all()
)

log_result(
    "adresse_du_domicile non vide",
    rh_df["adresse_du_domicile"].notna().all()
)

log_result(
    "moyen_de_deplacement non vide",
    rh_df["moyen_de_deplacement"].notna().all()
)

logger.info("=== FIN TEST RH ===")