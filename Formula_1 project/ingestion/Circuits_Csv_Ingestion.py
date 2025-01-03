# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_dat_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------



# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/Common_functions

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG teama1;

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import current_timestamp, lit

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
    .withColumnRenamed('alt', 'altitude') \
    .withColumn("data_source", lit(v_dat_source))

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG hive_metastore;
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS f1_processed
# MAGIC MANAGED LOCATION '/mnt/adlsgen2teama1dev1/processed';

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format( "parquet").saveAsTable('f1_processed.circuits')

# COMMAND ----------


