# config.py

AWS_REGION = "eu-west-3"
BUCKET = "sport-data-lake"
SLACK_ENABLED = True
SLACK_MAX_MESSAGES = 2   # nombre max de messages envoyés
SLACK_DEMO_MODE = True   # mode démo
RANDOM_SEED = 42
# Adresse entreprise
COMPANY_ADDRESS = "1362 Av. des Platanes, 34970 Lattes, France"

# Règles métier
PRIME_RATE = 0.05
WELLBEING_ACTIVITY_THRESHOLD = 15
WELLBEING_DAYS = 5

COMMUTE_THRESHOLDS = {
    "marche/running": 15,
    "velo/trottinette/autres": 25
}

# Chemins S3
KEY = "raw/rh/donnees_rh.xlsx"
KEY_SPORT = "raw/sport/donnees_sportive.xlsx"
RH_KEY = "staging/rh/rh_clean.csv"
RH_KEY_REJECTS="staging/rh/rh_rejects.csv"
SPORT_DECLARATIF_KEY = "staging/sport/sport_declaratif_clean.csv"
SPORT_KEY_REJECTS="staging/sport/sport_declaratif_rejects.csv"
ACTIVITIES_RAW_KEY = "raw/sport/sport_activities_generated.csv"
ACTIVITIES_CLEAN_KEY = "staging/sport/sport_activities_clean.csv"
OUTPUT_REJECTS_S3 = "staging/sport/sport_activities_rejects.csv"
SUMMARY_KEY = "curated/employee_sport_summary.csv"
COMMUTE_KEY = "curated/employee_commute_validation.csv"
FINANCIALS_KEY = "curated/employee_sport_financials.csv"

# Fichiers locaux
LOCAL_RH_CLEAN="data/sample/rh_clean.csv"
LOCAL_SPORT_CLEAN ="data/sample/sport_declaratif_clean.csv"
LOCAL_RH_REJECTS_FILE="data/sample/rh_rejects.csv"
LOCAL_SPORT_REJECTS_FILE="data/sample/sport_declaratif_rejects.csv"
LOCAL_ACTIVITIES_FILE = "data/sample/sport_activities_generated.csv"
LOCAL_ACTIVITIES_CLEAN_FILE = "data/sample/sport_activities_clean.csv"
LOCAL_ACTIVITIES_REJECTS_FILE = "data/sample/sport_activities_rejects.csv"
LOCAL_SUMMARY_FILE = "data/sample/employee_sport_summary.csv"
LOCAL_COMMUTE_FILE = "data/sample/employee_commute_validation.csv"
LOCAL_FINANCIALS_FILE = "data/sample/employee_sport_financials.csv"


DUCKDB_FILE = "db/sport_analytics.duckdb"

DUCKDB_SUMMARY_TABLE = "employee_sport_summary"
DUCKDB_FINANCIALS_TABLE = "employee_sport_financials"