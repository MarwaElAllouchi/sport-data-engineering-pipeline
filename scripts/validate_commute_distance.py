#validate_commute_distance.py
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import io
import time
import unicodedata
import pandas as pd
import boto3

from geopy.geocoders import Nominatim
from geopy.distance import geodesic
from utils.utils_logger import setup_logger
logger = setup_logger("validate_commute_distance")

from config import (
    BUCKET,
    AWS_REGION,
    RH_KEY,
    LOCAL_COMMUTE_FILE,
    COMMUTE_KEY,
    COMPANY_ADDRESS,
    COMMUTE_THRESHOLDS
)

s3 = boto3.client("s3", region_name=AWS_REGION)

OUTPUT_LOCAL = LOCAL_COMMUTE_FILE
OUTPUT_S3 = COMMUTE_KEY

geolocator = Nominatim(user_agent="sport_data_solution_poc")


def normalize_text(value):
    if pd.isna(value):
        return ""
    value = str(value).strip().lower()
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("utf-8")
    return value


def geocode_address(address):
    try:
        if pd.isna(address) or str(address).strip() == "":
            return None
        location = geolocator.geocode(str(address))
        time.sleep(1)  # éviter trop de requêtes rapprochées
        if location:
            return (location.latitude, location.longitude)
        return None
    except Exception:
        return None


# 1) Lire RH clean depuis S3
obj = s3.get_object(Bucket=BUCKET, Key=RH_KEY)
rh_df = pd.read_csv(io.BytesIO(obj["Body"].read()))

# 2) Vérifier colonnes attendues
required_columns = ["id_salarie", "adresse_du_domicile", "moyen_de_deplacement"]
missing_columns = [col for col in required_columns if col not in rh_df.columns]
if missing_columns:
    raise ValueError(f"Colonnes manquantes dans RH : {missing_columns}")

# 3) Préparer les colonnes utiles
rh_df["id_salarie"] = pd.to_numeric(rh_df["id_salarie"], errors="coerce")
rh_df["moyen_de_deplacement_normalise"] = rh_df["moyen_de_deplacement"].apply(normalize_text)

# 4) Géocoder l’adresse entreprise une seule fois
company_coords = geocode_address(COMPANY_ADDRESS)
if company_coords is None:
    raise ValueError("Impossible de géocoder l'adresse de l'entreprise.")

# 5) Cache adresses
address_cache = {}


def get_coords(address):
    key = str(address).strip() if not pd.isna(address) else ""
    if key in address_cache:
        return address_cache[key]
    coords = geocode_address(key)
    address_cache[key] = coords
    return coords


results = []

for _, row in rh_df.iterrows():
    emp_id = row["id_salarie"]
    address = row["adresse_du_domicile"]
    mode = row["moyen_de_deplacement"]
    mode_norm = row["moyen_de_deplacement_normalise"]

    home_coords = get_coords(address)

    distance_km = None
    geocoding_status = "ok"
    threshold_km = COMMUTE_THRESHOLDS.get(mode_norm)
    trajet_coherent = None
    anomalie_declaration = None

    if home_coords is None:
        geocoding_status = "adresse_non_trouvee"
    else:
        distance_km = round(geodesic(home_coords, company_coords).km, 2)

        # Seulement pour les modes sportifs définis dans COMMUTE_THRESHOLDS
        if threshold_km is not None:
            trajet_coherent = "oui" if distance_km <= threshold_km else "non"
            anomalie_declaration = "oui" if distance_km > threshold_km else "non"
        else:
            trajet_coherent = None
            anomalie_declaration = None

    results.append({
        "id_salarie": emp_id,
        "adresse_du_domicile": address,
        "moyen_de_deplacement": mode,
        "moyen_de_deplacement_normalise": mode_norm,
        "distance_km": distance_km,
        "distance_threshold_km": threshold_km,
        "trajet_coherent": trajet_coherent,
        "anomalie_declaration": anomalie_declaration,
        "geocoding_status": geocoding_status
    })

validation_df = pd.DataFrame(results)

# 6) Sauvegarde
validation_df.to_csv(OUTPUT_LOCAL, index=False)
s3.upload_file(OUTPUT_LOCAL, BUCKET, OUTPUT_S3)

logger.info("✅ Validation des trajets terminée")
logger.info("Nombre de salariés : %s", len(validation_df))
logger.info("Adresses non trouvées : %s", (validation_df["geocoding_status"] != "ok").sum())
logger.info("Anomalies détectées : %s", (validation_df["anomalie_declaration"] == "oui").sum())
logger.info("Fichier S3 : %s", f"s3://{BUCKET}/{OUTPUT_S3}")