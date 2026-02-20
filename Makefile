SHELL := /bin/bash

up:
	docker compose up -d

down:
	docker compose down -v

logs:
	docker compose logs -f --tail=200

bronze:
	docker compose run --rm spark-submit python jobs/01_ingest_bronze.py

silver:
	docker compose run --rm spark-submit python jobs/02_build_silver_features.py

gold:
	docker compose run --rm spark-submit python jobs/03_build_gold_training_set.py

all: bronze silver gold

train:
	docker compose run --rm spark-submit python jobs/04_train_and_export_submission.py

submit: train

elo:
	docker compose run --rm spark-submit python jobs/05_build_silver_elo.py

rolling:
	docker compose run --rm spark-submit python jobs/06_build_silver_rolling.py

expert:
	docker compose run --rm spark-submit python jobs/07_backtest_and_export_blend.py
