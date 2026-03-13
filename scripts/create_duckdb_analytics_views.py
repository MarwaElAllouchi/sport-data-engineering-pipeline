import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb
from utils.utils_logger import setup_logger
from config import DUCKDB_FILE

logger = setup_logger("create_duckdb_analytics_views")

con = duckdb.connect(DUCKDB_FILE)

logger.info("Connexion à DuckDB établie")

# 1) Vue KPI globale
con.execute("""
CREATE OR REPLACE VIEW vw_kpi_global AS
SELECT
    COUNT(*) AS nb_salaries,
    SUM(CASE WHEN eligible_bien_etre = 'oui' THEN 1 ELSE 0 END) AS nb_eligibles_bien_etre,
    SUM(jours_bien_etre) AS total_jours_bien_etre,
    SUM(CASE WHEN eligible_prime_sport = 'oui' THEN 1 ELSE 0 END) AS nb_eligibles_prime,
    ROUND(SUM(prime_sport), 2) AS total_prime_sport,
    SUM(CASE WHEN geocoding_status != 'ok' THEN 1 ELSE 0 END) AS nb_adresses_non_trouvees
FROM employee_sport_financials
""")
logger.info("Vue vw_kpi_global créée")

# 2) Vue analyse par BU
con.execute("""
CREATE OR REPLACE VIEW vw_bu_analysis AS
SELECT
    bu,
    COUNT(*) AS nb_salaries,
    ROUND(AVG(nb_activites_12_mois), 2) AS avg_activities,
    SUM(jours_bien_etre) AS total_jours_bien_etre,
    ROUND(SUM(prime_sport), 2) AS total_prime_sport
FROM employee_sport_financials
GROUP BY bu
ORDER BY bu
""")
logger.info("Vue vw_bu_analysis créée")

# 3) Vue analyse transport
con.execute("""
CREATE OR REPLACE VIEW vw_transport_analysis AS
SELECT
    moyen_de_deplacement,
    COUNT(*) AS nb_salaries,
    ROUND(AVG(distance_km), 2) AS avg_distance_km,
    SUM(CASE WHEN eligible_prime_sport = 'oui' THEN 1 ELSE 0 END) AS nb_eligibles_prime
FROM employee_sport_financials
GROUP BY moyen_de_deplacement
ORDER BY moyen_de_deplacement
""")
logger.info("Vue vw_transport_analysis créée")

# 4) Export CSV pour Power BI
con.execute("COPY (SELECT * FROM vw_kpi_global) TO 'data/powerbi/powerbi_kpi_global.csv' (HEADER, DELIMITER ',')")
con.execute("COPY (SELECT * FROM vw_bu_analysis) TO 'data/powerbi/powerbi_bu_analysis.csv' (HEADER, DELIMITER ',')")
con.execute("COPY (SELECT * FROM vw_transport_analysis) TO 'data/powerbi/powerbi_transport_analysis.csv' (HEADER, DELIMITER ',')")

logger.info("Exports CSV Power BI générés")
logger.info("Fichiers : powerbi_kpi_global.csv, powerbi_bu_analysis.csv, powerbi_transport_analysis.csv")

con.close()
logger.info("Fin création des vues analytiques")