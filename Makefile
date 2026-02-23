# ══════════════════════════════════════════════════════════════════════════════
# March Mania — Makefile
#
# Centralise toutes les commandes du projet pour Docker ET local.
# Usage : make <cible>
#
# Prérequis Docker : Docker Desktop + docker compose
# Prérequis local  : Python 3.12 + Spark installé + SPARK_HOME défini
# ══════════════════════════════════════════════════════════════════════════════

DOCKER_SUBMIT = docker compose run --rm spark-submit
PYTHON        = python
PIPELINE      = jobs/run_pipeline.py

# ── Infrastructure ─────────────────────────────────────────────────────────────

## Démarre MinIO + Spark master/worker en arrière-plan
up:
	docker compose up -d minio minio-init spark-master spark-worker-1
	@echo "✅  Cluster démarré. UIs :"
	@echo "    Spark   → http://localhost:8080"
	@echo "    MinIO   → http://localhost:9001"

## Arrête et supprime les conteneurs (conserve les données MinIO)
down:
	docker compose down

## Arrête + supprime TOUT (conteneurs + volume MinIO → données perdues)
clean:
	docker compose down -v
	@echo "⚠️  Volume MinIO supprimé."

## Reconstruit les images Spark (après modif du Dockerfile ou requirements.txt)
build:
	docker compose build --no-cache spark-master spark-worker-1 spark-submit

## Affiche les logs du cluster en temps réel
logs:
	docker compose logs -f spark-master spark-worker-1


# ── Pipeline complet (Docker) ──────────────────────────────────────────────────

## Lance le pipeline complet Bronze → Gold → HPO → Ensemble (Docker)
full:
	$(DOCKER_SUBMIT) $(PIPELINE) 1 12

## Ingestion Bronze uniquement
bronze:
	$(DOCKER_SUBMIT) $(PIPELINE) 1 1

## Silver complet (features, ELO, rolling, seeds, massey, SOS)
silver:
	$(DOCKER_SUBMIT) $(PIPELINE) 2 2
	$(DOCKER_SUBMIT) $(PIPELINE) 5 10

## Gold dataset
gold:
	$(DOCKER_SUBMIT) $(PIPELINE) 3 3

## HPO + ensemble
ensemble:
	$(DOCKER_SUBMIT) $(PIPELINE) 11 12

## Backtest expert + soumission
expert:
	$(DOCKER_SUBMIT) $(PIPELINE) 7 7


# ── Pipeline local (sans Docker) ──────────────────────────────────────────────
# Nécessite : SPARK_HOME défini + MinIO accessible ou mode fichier local

## Lance le pipeline complet en local
local-full:
	$(PYTHON) $(PIPELINE) 1 12

## Lance un job spécifique en local (usage : make local-job JOB=3)
local-job:
	$(PYTHON) $(PIPELINE) $(JOB)

## Installe les dépendances Python localement
install:
	pip install -r requirements.txt


# ── Utilitaires ───────────────────────────────────────────────────────────────

## Liste les jobs disponibles
jobs:
	$(PYTHON) $(PIPELINE)

## Vérification syntaxe Python (tous les jobs)
lint:
	$(PYTHON) -m py_compile jobs/*.py src/**/*.py
	@echo "✅  Aucune erreur de syntaxe."

## Crée les dossiers nécessaires (à faire avant le premier run)
init-dirs:
	mkdir -p data/input artifacts conf jars
	@echo "✅  Dossiers créés : data/input/, artifacts/, conf/, jars/"

.PHONY: up down clean build logs full bronze silver gold ensemble expert \
        local-full local-job install jobs lint init-dirs
