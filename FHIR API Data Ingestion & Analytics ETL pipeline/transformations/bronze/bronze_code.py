from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

volume_base_path = "/Volumes/fhir_api_data_catalog/raw/api_data"
checkpoint_path = "/Volumes/fhir_api_data_catalog/bronze/pipeline_checkpoints"
api_objects = ["Patient", "Observation", "Encounter","Condition"]
base_url = "https://hapi.fhir.org/baseR4"

def create_bronze_table(object):
    @dp.table(
        name=f'bronze_{object.lower()}',
        table_properties={"layer": "bronze"}
    )
    def ingest_data():
        schema_hints = "component ARRAY<STRUCT<valueQuantity:STRUCT<value:DOUBLE>>>, valueQuantity STRUCT<value:DOUBLE>"
        
        df_read=spark.readStream.format("cloudFiles")\
            .option('cloudFiles.format','json')\
            .option("cloudFiles.schemaLocation", f"{checkpoint_path}/{object}")\
            .option('cloudFiles.inferColumnTypes','true')\
            .option("cloudFiles.schemaEvolutionMode", "rescue")\
            .option("cloudFiles.schemaHints", schema_hints)\
            .load(f"{volume_base_path}/{object}/")

        df=df_read.withColumn("extraction_timestamp", current_timestamp())\
                .withColumn("api_url_or_params", lit(f"{base_url}/{object}/_history"))
        
        return df


for obj in api_objects:
    create_bronze_table(obj)


