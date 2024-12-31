# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG teama1;

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_schema = StructType([StructField("circuitId", IntegerType(), False),
                              StructField("circuitRef", StringType(), True),
                              StructField("name", StringType(), True),
                              StructField("location", StringType(), True),
                              StructField("country", StringType(), True),
                              StructField("lat", DoubleType(), True),
                              StructField("lng", DoubleType(), True),
                              StructField("alt", IntegerType(), True),
                              StructField("url", StringType(), True)])

# COMMAND ----------

raw_folder_path

# COMMAND ----------

circuits_df = spark.read.\
                option("header", True).\
                schema(circuits_schema).\
                csv(f'{raw_folder_path}/circuits.csv')

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# display(circuits_df)

# COMMAND ----------

circuits_sel_df = circuits_df.select('circuitId', 'circuitRef', 'name', 'location', 'country', 'lat', 'lng', 'alt')

# COMMAND ----------

circuits_renamed_df = circuits_sel_df.withColumnRenamed('circuitId', 'circuit_id') \
    .withColumnRenamed('circuitRef', 'circuit_ref') \
    .withColumnRenamed('lat', 'latitude') \
    .withColumnRenamed('lng', 'longitude') \
    .withColumnRenamed('alt', 'altitude')

# COMMAND ----------

circuits_final_df = circuits_renamed_df.withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet(f'{processed_folder_path}/circuits')
