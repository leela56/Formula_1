# Databricks notebook source
#  display(dbutils.fs.mounts())

# COMMAND ----------

# %fs
# ls /mnt/adlsgen2teama1dev1/raw

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import to_timestamp, col, lit, concat

# COMMAND ----------

races_schema = StructType([StructField('raceId', IntegerType(), True),
                           StructField('year', IntegerType(), True),
                           StructField('round', IntegerType(), True),
                           StructField('circuitId', IntegerType(), True),
                           StructField('name', StringType(), True),
                           StructField('date', StringType(), True),
                           StructField('time', StringType(), True),
                           StructField('url', StringType(), True)])

# COMMAND ----------

races_df = spark.read.option("header", True) \
                         .schema(races_schema) \
                         .csv('dbfs:/mnt/adlsgen2teama1dev1/raw/races.csv') 
                        

# COMMAND ----------

races_selc_df = races_df.select('raceId', 'year', 'round', 'circuitId', 'name', 'date', 'time')


# COMMAND ----------

races_renamed_df = races_selc_df.withColumnRenamed('raceId', 'race_id') \
                                .withColumnRenamed('year', 'race_year') \
                                .withColumnRenamed('circuitId', 'circuit_id')


# COMMAND ----------

races_final_df0 = races_renamed_df.withColumn('ingestion_date', current_timestamp())

# COMMAND ----------


races_final_df1 = races_final_df0.withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time'))))


# COMMAND ----------

races_final_df = races_final_df1.select('race_id', 'race_year', 'round', 'circuit_id', 'name', 'ingestion_date', 'race_timestamp')

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

races_final_df.write.mode("overwrite").parquet("/mnt/adlsgen2teama1dev1/processed/races")

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE teama1.bronze.races
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://teama1dev1@adlsgen2teama1.dfs.core.windows.net/processed/races';
# MAGIC
