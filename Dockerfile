FROM python:3.12-slim

# Empêcher Python de générer des fichiers .pyc et forcer l'affichage des logs
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Installation des dépendances système si nécessaire (ex: curl pour debug)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -U pip && \
    pip install --no-cache-dir -r requirements.txt

# On ne copie pas le code ici car Kestra le clone via Git dans le WorkingDirectory