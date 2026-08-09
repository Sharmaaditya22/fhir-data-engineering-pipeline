from pyspark import pipelines as dp
from pyspark.sql.functions import col


@dp.view(name="vw_silver_encounter_clean")
def clean_encounter():
        df_silver_encounter=spark.readStream.table("bronze_encounter") 
        df_silver_encounter_trans=df_silver_encounter.select(
            col("id").cast("string").alias("encounter_id"),
            col("meta.lastUpdated").cast("timestamp").alias("updated_at"),
            col("status"),
            
            col("subject").getField("reference").alias("patient_reference"),

            col("participant").getItem(0).getField("individual").getField("reference").alias("practitioner_reference"),

            col("class").getField("display").alias("encounter_class"),
            col("type").getItem(0).getField("text").alias("encounter_type"),
            col("reasonCode").getItem(0).getField("coding").getItem(0).getField("display").alias("primary_reason"),
            
            col("hospitalization").getField("admitSource").getField("coding").getItem(0).getField("display").alias("admit_source"),
            col("hospitalization").getField("dischargeDisposition").getField("coding").getItem(0).getField("display").alias("discharge_disposition"),
            
            col("period").getField("start").cast("timestamp").alias("admission_time"),
            col("period").getField("end").cast("timestamp").alias("discharge_time"),
            
            col("extraction_timestamp"),
            col("api_url_or_params"),
            col("_rescued_data")
        )\
        .filter(col("encounter_id").isNotNull()) 

        return df_silver_encounter_trans

dp.create_streaming_table(
    name="silver.silver_encounter",
    comment="Cleaned, SCD Type 2 Encounter records tracking patient visits"
)


dp.apply_changes(
    target = "silver.silver_encounter",
    source = "vw_silver_encounter_clean",
    keys = ["encounter_id"],
    sequence_by = col("updated_at"),
    stored_as_scd_type = 2, 
    track_history_except_column_list = [
        "extraction_timestamp", 
        "api_url_or_params", 
        "_rescued_data"
    ]
)