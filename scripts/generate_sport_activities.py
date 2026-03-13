import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import io
import random
from datetime import timedelta
import pandas as pd
import boto3
from faker import Faker
from utils.utils_logger import setup_logger


from config import (
BUCKET ,LOCAL_ACTIVITIES_FILE,ACTIVITIES_RAW_KEY ,
RH_KEY 

)
logger = setup_logger("generation_activites_sportives")
OUTPUT_LOCAL = LOCAL_ACTIVITIES_FILE
OUTPUT_S3_KEY = ACTIVITIES_RAW_KEY

fake = Faker("fr_FR")

s3 = boto3.client("s3", region_name="eu-west-3")

# 1) Lire le fichier RH propre depuis S3
obj = s3.get_object(Bucket=BUCKET, Key=RH_KEY)
rh_bytes = obj["Body"].read()
rh_df = pd.read_csv(io.BytesIO(rh_bytes))

# 2) Liste des salariés
employee_ids = rh_df["id_salarie"].dropna().unique().tolist()

# 3) Types de sport + paramètres
sports_config = {
    "course_a_pied": {"distance_range": (3000, 15000), "speed_min": 2.2, "speed_max": 3.8},
    "velo": {"distance_range": (5000, 50000), "speed_min": 4.0, "speed_max": 9.0},
    "marche": {"distance_range": (2000, 12000), "speed_min": 1.0, "speed_max": 1.8},
    "randonnee": {"distance_range": (5000, 20000), "speed_min": 0.8, "speed_max": 1.5},
    "natation": {"distance_range": (500, 3000), "speed_min": 0.4, "speed_max": 1.2},
}

# 4) Génération des activités
activities = []
activity_id = 1

for emp_id in employee_ids:

    # Faker génère un nombre réaliste d'activités
    nb_activities = random.randint(8, 40)

    for _ in range(nb_activities):

        sport_type = random.choice(list(sports_config.keys()))
        config = sports_config[sport_type]

        # Faker génère une date sur les 12 derniers mois
        date_debut = fake.date_time_between(start_date="-12M", end_date="now")

        # distance réaliste
        distance_m = random.randint(*config["distance_range"])

        # vitesse réaliste
        speed = random.uniform(config["speed_min"], config["speed_max"])

        temps_ecoule_s = int(distance_m / speed)

        date_fin = date_debut + timedelta(seconds=temps_ecoule_s)

        # Faker génère un commentaire sportif réaliste
        commentaire = fake.sentence(nb_words=6)

        activities.append({
            "id_activite": activity_id,
            "id_salarie": emp_id,
            "date_debut": date_debut.strftime("%Y-%m-%d %H:%M:%S"),
            "date_fin": date_fin.strftime("%Y-%m-%d %H:%M:%S"),
            "sport_type": sport_type,
            "distance_m": distance_m,
            "temps_ecoule_s": temps_ecoule_s,
            "commentaire": commentaire
        })

        activity_id += 1

# 5) DataFrame
activities_df = pd.DataFrame(activities)

# 6) Sauvegarde locale
activities_df.to_csv(OUTPUT_LOCAL, index=False)

# 7) Upload vers S3
s3.upload_file(OUTPUT_LOCAL, BUCKET, OUTPUT_S3_KEY)

logger.info("✅ Génération terminée")
logger.info("Nombre d'activités générées : %s", len(activities_df))
logger.info("Fichier local : %s", OUTPUT_LOCAL)
logger.info("Fichier S3 : %s", f"s3://{BUCKET}/{OUTPUT_S3_KEY}")