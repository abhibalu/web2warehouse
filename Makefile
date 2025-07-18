# Activate the virtual environment and add current directory to PYTHONPATH
define ACTIVATE
	source .venv/bin/activate && \
	export PYTHONPATH=$$PYTHONPATH:$(PWD)
endef

run_all:
	$(ACTIVATE) && \
	for script in pipelines/scrape_props.py pipelines/minio_upload.py pipelines/delta_lake.py pipelines/duckdb_ingestion.py; do \
		echo "Running $$script..."; \
		python $$script || exit 1; \
	done

run_prop_scrape:
	$(ACTIVATE) && \
	echo "Running scrape_props.py..." && \
	python pipelines/scrape_props.py

run_delta:
	$(ACTIVATE) && \
	echo "Running delta_lake.py..." && \
	python pipelines/delta_lake.py

duckdb_bootstrap:
	@envsubst < sql/bootstrap_duckdb.sql | duckdb data/warehouse.duckdb