README.md – Sport Data Engineering Pipeline
🏃 Sport Data Engineering Pipeline

POC de Data Engineering visant à analyser les avantages sportifs des salariés d'une entreprise.

Le projet met en place un pipeline de données complet :

ingestion

transformation

contrôle qualité

validation métier

calcul d’impact financier

stockage analytique

visualisation

Le pipeline est orchestré avec Kestra et les données sont analysées dans Power BI.

📊 Architecture du projet
Architecture logique
🏗 Architecture technique
Orchestration

Kestra

Responsabilités :

orchestration du pipeline

exécution des scripts Python

gestion des erreurs

monitoring

alerting

Traitement des données

Technologies :

Python

Pandas

Faker (génération données sportives)

Geopy (validation distance domicile-travail)

Scripts principaux :

clean_rh.py
clean_sport.py
generate_sport_activities.py
clean_generated_activities.py
build_employee_sport_summary.py
validate_commute_distance.py
compute_financial_impact.py

Chaque étape possède un test de qualité dédié :

tests/test_rh.py
tests/test_activities.py
tests/test_summary.py
tests/test_commute.py
tests/test_financials.py

Approche utilisée :

Fail-Fast Pipeline

1 script de traitement
→ 1 script de test

Si un test échoue, le pipeline s'arrête.

🗄 Data Lake

Stockage des données dans Amazon S3.

Structure :

S3
│
├── raw
│
├── staging
│
└── curated

Objectifs :

historiser les données

séparer ingestion et transformation

faciliter la traçabilité

🧠 Data Warehouse
Implémentation POC

Le projet utilise DuckDB comme moteur analytique local.

Avantages :

rapide

léger

sans serveur

idéal pour un prototype

Les tables analytiques sont chargées via :

load_to_duckdb.py

Puis les vues analytiques sont créées :

create_duckdb_analytics_views.py
Architecture cible (Production)

Dans une architecture cloud, DuckDB serait remplacé par Amazon Redshift.

POC	Production
DuckDB	Amazon Redshift

Redshift permet :

stockage massif

requêtes analytiques distribuées

intégration native AWS

connexion BI directe

📈 Visualisation

Les données analytiques sont exportées vers Power BI.

Fichiers utilisés :

powerbi_kpi_global.csv
powerbi_bu_analysis.csv
powerbi_transport_analysis.csv

Dashboard réalisé :

KPIs

nombre total de salariés

salariés éligibles bien-être

total jours bien-être

salariés éligibles prime sportive

impact financier total

adresses non géocodées

Analyses

Analyse par Business Unit

Impact financier des primes par département.

Analyse des modes de transport

marche / running

vélo

transports en commun

véhicule thermique / électrique

⚙️ Pipeline orchestration

Pipeline Kestra :

Git Clone
↓
Clean RH
↓
Test RH
↓
Clean Sport
↓
Generate Activities
↓
Clean Activities
↓
Test Activities
↓
Build Employee Summary
↓
Test Summary
↓
Validate Commute Distance
↓
Test Commute
↓
Compute Financial Impact
↓
Test Financials
↓
Load to DuckDB
↓
Create Analytics Views
↓
Check Volumetry
📡 Monitoring & Alerting

Pour garantir la fiabilité du pipeline :

Monitoring

Suivi des indicateurs :

nombre de lignes traitées

volume de données

durée d’exécution

nombre d’anomalies détectées

Rapport généré :

volumetry_report.json
Alerting

En cas d'échec du pipeline :

log d’erreur Kestra

envoi d’email automatique

Email contient :

ID de l'exécution

tâche en erreur

date de l'incident

🐳 Environnement d'exécution

Les scripts sont exécutés dans un container Docker :

sport-pipeline:latest

Contient :

Python

pandas

boto3

faker

geopy

duckdb

📅 Planification

Le pipeline peut être exécuté :

manuellement

planifié mensuellement

Kestra trigger :

@monthly
📁 Structure du projet
sport-data-engineering-pipeline

scripts/
    clean_rh.py
    clean_sport.py
    generate_sport_activities.py
    clean_generated_activities.py
    build_employee_sport_summary.py
    validate_commute_distance.py
    compute_financial_impact.py
    load_to_duckdb.py
    create_duckdb_analytics_views.py
    check_volumetry.py

tests/
    test_rh.py
    test_activities.py
    test_summary.py
    test_commute.py
    test_financials.py

data/

exports/

kestra/
    pipeline.yml
🎯 Objectifs du projet

Ce projet démontre les compétences suivantes :

conception d’un pipeline de données

orchestration avec Kestra

gestion de qualité de données

création d’un Data Lake

mise en place d’un Data Warehouse

création de KPIs métiers

réalisation d’un dashboard analytique

👩‍💻 Auteur

Projet réalisé dans le cadre de la formation :

Data Engineer – OpenClassrooms