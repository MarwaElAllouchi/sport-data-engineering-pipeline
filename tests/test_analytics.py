import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb
from utils.utils_logger import setup_logger
from config import DUCKDB_FILE, DUCKDB_SUMMARY_TABLE, DUCKDB_FINANCIALS_TABLE

logger = setup_logger("test_analytics")


def log_result(test_name, passed, detail=""):
    if passed:
        logger.info(f"PASS - {test_name}")
        if detail:
            logger.info(detail)
    else:
        logger.error(f"FAIL - {test_name}")
        if detail:
            logger.error(detail)
        raise Exception(f"Test échoué : {test_name}")


logger.info("=== TEST ANALYTICS ===")

log_result(
    "fichier DuckDB existe",
    os.path.exists(DUCKDB_FILE),
    f"Chemin : {DUCKDB_FILE}"
)

con = duckdb.connect(DUCKDB_FILE)

summary_exists = con.execute(f"""
SELECT COUNT(*)
FROM information_schema.tables
WHERE table_name = '{DUCKDB_SUMMARY_TABLE}'
""").fetchone()[0] == 1

log_result(
    "table summary existe",
    summary_exists,
    DUCKDB_SUMMARY_TABLE
)

financials_exists = con.execute(f"""
SELECT COUNT(*)
FROM information_schema.tables
WHERE table_name = '{DUCKDB_FINANCIALS_TABLE}'
""").fetchone()[0] == 1

log_result(
    "table financials existe",
    financials_exists,
    DUCKDB_FINANCIALS_TABLE
)

vw_kpi_exists = con.execute("""
SELECT COUNT(*)
FROM information_schema.tables
WHERE table_name = 'vw_kpi_global'
""").fetchone()[0] == 1

log_result(
    "vue vw_kpi_global existe",
    vw_kpi_exists
)

vw_bu_exists = con.execute("""
SELECT COUNT(*)
FROM information_schema.tables
WHERE table_name = 'vw_bu_analysis'
""").fetchone()[0] == 1

log_result(
    "vue vw_bu_analysis existe",
    vw_bu_exists
)

vw_transport_exists = con.execute("""
SELECT COUNT(*)
FROM information_schema.tables
WHERE table_name = 'vw_transport_analysis'
""").fetchone()[0] == 1

log_result(
    "vue vw_transport_analysis existe",
    vw_transport_exists
)

summary_count = con.execute(
    f"SELECT COUNT(*) FROM {DUCKDB_SUMMARY_TABLE}"
).fetchone()[0]

log_result(
    "table summary non vide",
    summary_count > 0,
    f"{summary_count} lignes"
)

financials_count = con.execute(
    f"SELECT COUNT(*) FROM {DUCKDB_FINANCIALS_TABLE}"
).fetchone()[0]

log_result(
    "table financials non vide",
    financials_count > 0,
    f"{financials_count} lignes"
)

kpi_count = con.execute("SELECT COUNT(*) FROM vw_kpi_global").fetchone()[0]

log_result(
    "vue KPI non vide",
    kpi_count > 0,
    f"{kpi_count} ligne(s)"
)

con.close()

logger.info("=== FIN TEST ANALYTICS ===")