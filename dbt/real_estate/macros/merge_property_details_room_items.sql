{% macro merge_property_details_room_items() %}

    {% set count_query %}
        SELECT COUNT(*) AS cnt FROM delta_scan('s3://delta/delta_table/silver/delta_clean/property_details_room_items')
    {% endset %}

    {% set count_result = run_query(count_query) %}
    {% if count_result %}
        {% set num_rows = count_result.columns[0].values()[0] %}
        {{ print("✅ Source Delta table is accessible. Row count: " ~ num_rows) }}
    {% else %}
        {{ exceptions.raise_compiler_error("❌ Could not query Delta table. Check secret, endpoint, or path.") }}
    {% endif %}

    {{ print("🟡 Running MERGE statement...") }}

    MERGE INTO property_details_room_items_mt AS target
    USING (
        SELECT * FROM delta_scan('s3://delta/delta_table/silver/delta_clean/property_details_room_items')
    ) AS source
    ON target.id = source.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *;

    {{ print("✅ MERGE completed.") }}

{% endmacro %}
