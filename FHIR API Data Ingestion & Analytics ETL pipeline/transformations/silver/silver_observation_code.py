from pyspark import pipelines as dp
from pyspark.sql.functions import col


@dp.view(name="vw_silver_observation_clean")
def clean_observation():
        df_silver_observation=spark.readStream.table("bronze_observation") 
        df_silver_observation_trans=df_silver_observation.select(

            col("id").cast("string").alias("observation_id"),
            col("meta.lastUpdated").cast("timestamp").alias("updated_at"),
            col("status"),
            
            col("subject").getField("reference").alias("patient_reference"),
            col("encounter").getField("reference").alias("encounter_reference"),
            
            col("category").getItem(0).getField("coding").getItem(0).getField("display").alias("category_name"),
            col("code").getField("coding").getItem(0).getField("display").alias("observation_name"),
            
            col("effectiveDateTime").cast("timestamp").alias("effective_timestamp"),
            col("issued").cast("timestamp").alias("issued_timestamp"),
            
            col("valueQuantity").getField("value").cast("double").alias("value_numeric"),
            col("valueString").alias("value_text"),
            col("valueBoolean").cast("boolean").alias("value_boolean"),
            
            col("extraction_timestamp"),
            col("api_url_or_params"),
            col("_rescued_data")
        )\
            .filter(col("observation_id").isNotNull()) 

        return df_silver_observation_trans


dp.create_streaming_table(
    name="silver.silver_observation",
    comment="Cleaned, SCD Type 2 Observation records with history tracking"
)


dp.apply_changes(
    target = "silver.silver_observation",
    source = "vw_silver_observation_clean",
    keys = ["observation_id"],
    sequence_by = col("updated_at"),
    stored_as_scd_type = 2, 
    track_history_except_column_list = [
        "extraction_timestamp", 
        "api_url_or_params", 
        "_rescued_data"
    ]
)