from pyspark import pipelines as dp
from pyspark.sql.functions import col, split, datediff, current_date


@dp.table(
    name="gold.gold_patient_current_snapshot",
    comment="Current active state of all patients, optimized for demographic reporting"
)
def gold_patient_current():
        df_gold_patient=spark.read.table("silver.silver_patient")

        df_gold_patient_trans=df_gold_patient.filter(col("__END_AT").isNull())\
        .withColumn("current_age", datediff(current_date(), col("birth_date")) / 365.25)

        return df_gold_patient_trans



@dp.table(
    name="gold.gold_clinical_encounter_master",
    comment="Denormalized view joining Encounters, Patients, and Conditions"
)
def gold_clinical_master():

    df_patient = spark.read.table("silver.silver_patient").filter(col("__END_AT").isNull())
    df_encounter = spark.read.table("silver.silver_encounter").filter(col("__END_AT").isNull())
    df_condition = spark.read.table("silver.silver_condition").filter(col("__END_AT").isNull())

    df_encounter = df_encounter.withColumn("join_patient_id", split(col("patient_reference"), "/").getItem(1))
    df_condition = df_condition.withColumn("join_encounter_id", split(col("encounter_reference"), "/").getItem(1))

    joined_df = (
        df_encounter.alias("e")
        .join(
            df_patient.alias("p"), 
            col("e.join_patient_id") == col("p.patient_id"), 
            "left"
        )
        .join(
            df_condition.alias("c"), 
            col("e.encounter_id") == col("c.join_encounter_id"), 
            "left"
        )
        .select(
            col("e.encounter_id"),
            col("e.encounter_class"),
            col("e.admission_time"),
            col("e.discharge_time"),
            datediff(col("e.discharge_time"), col("e.admission_time")).alias("length_of_stay_days"),
            
            col("e.admit_source"),
            col("e.discharge_disposition"),
            
            col("p.patient_id"),
            col("p.gender"),
            col("p.city"),
            col("p.state"),
            
            col("p.marital_status"),
            
            col("c.condition_name"),
            col("c.clinical_status"),
            col("c.severity")
        )
    )
    
    return joined_df




@dp.table(
name="gold.gold_observation_analytics",
comment="Standalone analytics view for patient observations, vitals, and lab results"
)
def gold_observation():
    df_obs = spark.read.table("silver.silver_observation").filter(col("__END_AT").isNull())
    df_patient = spark.read.table("silver.silver_patient").filter(col("__END_AT").isNull())

    df_obs = df_obs.withColumn("join_patient_id", split(col("patient_reference"), "/").getItem(1))
    df_obs = df_obs.withColumn("join_encounter_id", split(col("encounter_reference"), "/").getItem(1))

    joined_df = (
        df_obs.alias("o")
        .join(
            df_patient.alias("p"), 
            col("o.join_patient_id") == col("p.patient_id"), 
            "left"
        )
        .select(
            col("o.observation_id"),
            col("o.join_patient_id").alias("patient_id"),
            col("o.join_encounter_id").alias("encounter_id"),
            
            col("p.gender"),
            col("p.birth_date"),
            
            col("o.category_name"),
            col("o.observation_name"),
            col("o.effective_timestamp"),
            col("o.status"),
            
            col("o.value_numeric"),
            col("o.value_text"),
            col("o.value_boolean")
        )
    )
    
    return joined_df