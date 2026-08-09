from pyspark import pipelines as dp
from pyspark.sql.functions import *


@dp.view(name="vw_silver_patient_clean")
def clean_patient():
        df_silver_patient=spark.readStream.table("bronze_patient") 
        df_silver_patient_trans=df_silver_patient.select(
            col("id").cast("string").alias("patient_id"),
            col("meta.lastUpdated").cast("timestamp").alias("updated_at"),
            
            col("name").getItem(0).getField("family").alias("last_name"),
            col("name").getItem(0).getField("given").getItem(0).alias("first_name"),
            
            col("birthDate").cast("date").alias("birth_date"),
            col("gender"),
            col("active").cast("boolean").alias("is_active"),
            col("deceasedBoolean").cast("boolean").alias("is_deceased"),

            col("maritalStatus").getField("text").alias("marital_status"),
            
            col("telecom").getItem(0).getField("value").alias("primary_phone_or_email"),
            col("address").getItem(0).getField("city").alias("city"),
            col("address").getItem(0).getField("state").alias("state"),
            col("address").getItem(0).getField("country").alias("country"),
            col("address").getItem(0).getField("postalCode").alias("postal_code"),
            
            col("communication").getItem(0).getField("language").getField("text").alias("primary_language"),
            
            col("extraction_timestamp"),
            col("api_url_or_params"),
            col("_rescued_data")
        )\
        .filter(col("patient_id").isNotNull())

        return df_silver_patient_trans


    

dp.create_streaming_table(
    name="silver.silver_patient",
    comment="Cleaned, SCD Type 2 Patient records with history tracking"
)

dp.apply_changes(
    target = "silver.silver_patient",
    source = "vw_silver_patient_clean",
    keys = ["patient_id"],
    sequence_by = col("updated_at"),
    stored_as_scd_type = 2, 
    track_history_except_column_list = [
        "extraction_timestamp", 
        "api_url_or_params", 
        "_rescued_data"
    ]
)