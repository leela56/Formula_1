# Databricks notebook source
constructors_schema = "constructorID INT, constructorRef STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read \
                       .schema(constructors_schema) \
                        .json("/mnt/adlsgen2teama1dev1/raw/constructors.json")

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------


constructor_dropped_df = constructors_df.drop(col('url'))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp


# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("/mnt/adlsgen2a1dev1/processed/constructors")
