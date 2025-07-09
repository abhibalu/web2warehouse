VENV_ACTIVATE = source .venv/bin/activate

SCRIPTS = \
    /Users/abhijithm/Documents/Code/Scrapping/real_estate_pipeline/pipelines/scrape_props.py \
    /Users/abhijithm/Documents/Code/Scrapping/real_estate_pipeline/pipelines/minio_upload.py \
    /Users/abhijithm/Documents/Code/Scrapping/real_estate_pipeline/pipelines/delta_lake.py

run_all:
	$(VENV_ACTIVATE) && \
	for script in $(SCRIPTS); do \
		echo "Running $$script..."; \
		python $$script || exit 1; \
	done
