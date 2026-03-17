import sys
import os
import io
import json
import time
from dotenv import load_dotenv
import pandas as pd
import boto3
import requests

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.utils_logger import setup_logger
from config import (
    BUCKET,
    AWS_REGION,
    ACTIVITIES_CLEAN_KEY,
    SLACK_ENABLED,
    SLACK_MAX_MESSAGES,
    SLACK_DEMO_MODE
)
load_dotenv()
logger = setup_logger("publish_slack_activities")
s3 = boto3.client("s3", region_name=AWS_REGION)

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")


def mask_employee_id(employee_id):
    value = str(employee_id)
    if len(value) <= 2:
        return "**"
    return value[:2] + "***"


def format_message(row):
    employee_ref = mask_employee_id(row["id_salarie"])
    commentaire = "" if pd.isna(row["commentaire"]) else str(row["commentaire"])

    return (
        f"🏅 Nouvelle activité sportive !\n"
        f"Référence salarié : {employee_ref}\n"
        f"Sport : {row['sport_type']}\n"
        f"Distance : {row['distance_m']} m\n"
        f"Durée : {row['temps_ecoule_s']} s\n"
        f"Début : {row['date_debut']}\n"
        f"Fin : {row['date_fin']}\n"
        f"Commentaire : {commentaire}"
    )


def send_to_slack(message, max_retries=3):
    payload = {"text": message}

    for _ in range(max_retries):
        response = requests.post(
            SLACK_WEBHOOK_URL,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10
        )

        if response.status_code == 200:
            return

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 1))
            logger.warning("Slack rate limited. Waiting %s second(s)...", retry_after)
            time.sleep(retry_after)
            continue

        raise ValueError(
            f"Erreur Slack webhook : code={response.status_code}, réponse={response.text}"
        )

    raise ValueError("Échec après plusieurs tentatives à cause du rate limiting Slack.")


if not SLACK_ENABLED:
    logger.info("Publication Slack désactivée dans config.py")
    sys.exit(0)

if not SLACK_WEBHOOK_URL:
    raise ValueError("La variable d'environnement SLACK_WEBHOOK_URL est absente.")

obj = s3.get_object(Bucket=BUCKET, Key=ACTIVITIES_CLEAN_KEY)
activities_df = pd.read_csv(io.BytesIO(obj["Body"].read()))

if activities_df.empty:
    logger.info("Aucune activité à publier sur Slack.")
    sys.exit(0)

published_count = 0

if SLACK_DEMO_MODE:
    activities_df = activities_df.head(SLACK_MAX_MESSAGES)
    logger.info("Mode démo actif : envoi limité à %s messages", len(activities_df))

for _, row in activities_df.iterrows():
    message = format_message(row)
    send_to_slack(message)
    time.sleep(1.1)
    published_count += 1

logger.info("✅ Publication Slack terminée")
logger.info("Nombre de messages envoyés : %s", published_count)