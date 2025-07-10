run_all:
	source .venv/bin/activate && \
	export PYTHONPATH=$$PYTHONPATH:$(PWD) && \
	for script in pipelines/scrape_props.py pipelines/minio_upload.py pipelines/delta_lake.py; do \
		echo "Running $$script..."; \
		python $$script || exit 1; \
	done
