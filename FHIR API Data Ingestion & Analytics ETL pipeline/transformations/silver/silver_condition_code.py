from pyspark import pipelines as dp
from pyspark.sql.functions import col


@dp.view(name="vw_silver_condition_clean")
def clean_condition():
        df_silver_condition=spark.readStream.table("bronze_condition") 
        df_silver_condition_trans=df_silver_condition.select(
            col("id").cast("string").alias("condition_id"),
            col("meta.lastUpdated").cast("timestamp").alias("updated_at"),
            
            col("subject").getField("reference").alias("patient_reference"),
            col("encounter").getField("reference").alias("encounter_reference"),
            
            col("clinicalStatus").getField("coding").getItem(0).getField("code").alias("clinical_status"),
            col("verificationStatus").getField("coding").getItem(0).getField("code").alias("verification_status"),
            col("severity").getField("coding").getItem(0).getField("display").alias("severity"),
            
            col("code").getField("coding").getItem(0).getField("display").alias("condition_name"),
            
            col("onsetDateTime").cast("timestamp").alias("onset_time"),
            col("recordedDate").cast("timestamp").alias("recorded_time"),
            
            col("extraction_timestamp"),
            col("api_url_or_params"),
            col("_rescued_data")
        )\
            .filter(col("condition_id").isNotNull()) 

        return df_silver_condition_trans


dp.create_streaming_table(
    name="silver.silver_condition",
    comment="Cleaned, SCD Type 2 Condition records tracking patient diagnoses"
)


dp.apply_changes(
    target = "silver.silver_condition",
    source = "vw_silver_condition_clean",
    keys = ["condition_id"],
    sequence_by = col("updated_at"),
    stored_as_scd_type = 2, 
    track_history_except_column_list = [
        "extraction_timestamp", 
        "api_url_or_params", 
        "_rescued_data"
    ]
)