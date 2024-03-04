.PHONY: all

all: 
	make -j 3 setup run_prefect run_pipeline

run_pipeline: 
	sleep 3
	python3 de-project-pipeline.py

run_prefect: 
	prefect server start --host 0.0.0.0

setup: 
	pip install -r requirements.txt
	docker-compose -f de-project-compose.yaml up -d  
