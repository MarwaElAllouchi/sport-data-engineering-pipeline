import io
import pandas as pd
import boto3
from utils.utils_cleaning import ensure_parent_dir,clean_columns, remove_duplicates, split_clean_rejects, standardize_text

from config import (RH_KEY ,
                     BUCKET ,ACTIVITIES_RAW_KEY,
                     ACTIVITIES_CLEAN_KEY,
                     LOCAL_ACTIVITIES_CLEAN_FILE,
                     LOCAL_ACTIVITIES_REJECTS_FILE,OUTPUT_REJECTS_S3)

ACTIVITIES_KEY = ACTIVITIES_RAW_KEY
OUTPUT_CLEAN_LOCAL = LOCAL_ACTIVITIES_CLEAN_FILE 
OUTPUT_REJECTS_LOCAL = LOCAL_ACTIVITIES_REJECTS_FILE 

OUTPUT_CLEAN_S3 = ACTIVITIES_CLEAN_KEY
 
s3 = boto3.client("s3", region_name="eu-west-3")

# 1) Lire activités générées
obj_activities = s3.get_object(Bucket=BUCKET, Key=ACTIVITIES_KEY)
activities_bytes = obj_activities["Body"].read()
df = pd.read_csv(io.BytesIO(activities_bytes))

# 2) Lire RH propre
obj_rh = s3.get_object(Bucket=BUCKET, Key=RH_KEY)
rh_bytes = obj_rh["Body"].read()
rh_df = pd.read_csv(io.BytesIO(rh_bytes))

# 3) Nettoyage générique
df = clean_columns(df)
df = remove_duplicates(df)

rh_df = clean_columns(rh_df)
rh_df = remove_duplicates(rh_df)

print("Colonnes activités :", df.columns.tolist())

# 4) Vérifier les colonnes obligatoires
required_columns = [
    "id_activite",
    "id_salarie",
    "date_debut",
    "date_fin",
    "sport_type",
    "distance_m",
    "temps_ecoule_s",
    "commentaire"
]

missing_columns = [col for col in required_columns if col not in df.columns]
if missing_columns:
    raise ValueError(f"Colonnes manquantes dans le fichier d'activités : {missing_columns}")

if "id_salarie" not in rh_df.columns:
    raise ValueError("La colonne 'id_salarie' est absente du fichier RH propre")

# 5) Conversions de types
df["id_activite"] = pd.to_numeric(df["id_activite"], errors="coerce")
df["id_salarie"] = pd.to_numeric(df["id_salarie"], errors="coerce")
df["distance_m"] = pd.to_numeric(df["distance_m"], errors="coerce")
df["temps_ecoule_s"] = pd.to_numeric(df["temps_ecoule_s"], errors="coerce")
df["date_debut"] = pd.to_datetime(df["date_debut"], errors="coerce")
df["date_fin"] = pd.to_datetime(df["date_fin"], errors="coerce")

# 6) Normalisation légère du texte
df["sport_type"] = standardize_text(df["sport_type"])
df["commentaire"] = df["commentaire"].fillna("").astype(str).str.strip()

# 7) Référentiel RH
valid_employee_ids = set(
    pd.to_numeric(rh_df["id_salarie"], errors="coerce")
    .dropna()
    .astype(int)
    .tolist()
)

# 8) Règles de rejet
reject_mask = (
    df["id_activite"].isna()
    | df["id_salarie"].isna()
    | df["date_debut"].isna()
    | df["date_fin"].isna()
    | (df["date_fin"] <= df["date_debut"])
    | df["distance_m"].isna()
    | (df["distance_m"] < 0)
    | df["temps_ecoule_s"].isna()
    | (df["temps_ecoule_s"] < 0)
    | (~df["id_salarie"].fillna(-1).astype(int).isin(valid_employee_ids))
)

clean, rejects = split_clean_rejects(df, reject_mask)

# 9) Optionnel : remettre les dates dans un format propre
clean["date_debut"] = clean["date_debut"].dt.strftime("%Y-%m-%d %H:%M:%S")
clean["date_fin"] = clean["date_fin"].dt.strftime("%Y-%m-%d %H:%M:%S")

if not rejects.empty:
    rejects["date_debut"] = rejects["date_debut"].astype(str)
    rejects["date_fin"] = rejects["date_fin"].astype(str)

# 10) Sauvegarde locale
ensure_parent_dir(OUTPUT_CLEAN_LOCAL)
ensure_parent_dir(OUTPUT_REJECTS_LOCAL)

clean.to_csv(OUTPUT_CLEAN_LOCAL, index=False)
rejects.to_csv(OUTPUT_REJECTS_LOCAL, index=False)

# 11) Upload S3
s3.upload_file(OUTPUT_CLEAN_LOCAL, BUCKET, OUTPUT_CLEAN_S3)
s3.upload_file(OUTPUT_REJECTS_LOCAL, BUCKET, OUTPUT_REJECTS_S3)

print("✅ Activités sportives nettoyées")
print("Lignes propres :", len(clean))
print("Lignes rejetées :", len(rejects))