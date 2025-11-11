Real Estate Analytics dbt Project

Overview
This project models scraped real-estate listings into a layered warehouse (staging → intermediate → marts) on DuckDB with a dbt-native SCD2 snapshot for pricing and status history. It powers dimensional analysis for inventory, pricing trends, locations, and agents.

Key layers
- Staging (schema: staging): Typed, cleaned inputs from lake sources.
- Intermediate (schema: intermediate): Conformed, deduplicated tables; SCD2 assembled from dbt snapshots.
- Marts (schema: marts): Dimensions and facts for BI, plus curated aggregates.
- Snapshots (schema: snapshots): dbt SCD2 for price/status.

Core models
- staging.stg_property_flat: Typed projection of property records from lake ext_property_flat with snapshot_date.
- intermediate.int_property_base: Latest-deduped property attributes per property_id.
- snapshots.property_pricing_status: dbt snapshot of price/status changes by property_id.
- intermediate.int_property_snapshot: Thin wrapper around snapshot with valid_from/valid_to.
- marts.dims.dim_property_current: Current property attributes with latest status.
- marts.dims.dim_agent: Distinct agent dimension.
- marts.dims.dim_location: Distinct postcode/location dimension.
- marts.facts.fact_property_snapshot: One row per validity interval with price/status.
- marts.mart_postcode_price_analysis: Price metrics by postcode (avg, median, p95, counts).
- marts.mart_agent_performance: Basic agent KPIs (counts, actives, avg price).

Running
From this directory:
- dbt deps           # if packages are used
- dbt snapshot       # builds/updates SCD2
- dbt build          # runs models + tests
- dbt docs generate  # builds documentation site
- dbt docs serve     # serves documentation locally

Common checks
- Ensure snapshot has rows:
  select * from snapshots.property_pricing_status limit 20;
- Validate dedup base:
  select property_id, count(*) from intermediate.int_property_base group by 1 having count(*) > 1;
- Inspect marts:
  select * from marts.mart_postcode_price_analysis limit 20;
  select * from marts.mart_agent_performance limit 20;

Notes
- If quantile/median functions differ by DuckDB version, switch to quantile_cont.
- Adjust accepted status values/tests to match source reality.

