# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/Common_functions"

# COMMAND ----------

constructors_schema = "constructorID INT, constructorRef STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read \
                       .schema(constructors_schema) \
                        .json(f"{raw_folder_path}/constructors.json")

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------


constructor_dropped_df = constructors_df.drop(col('url'))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit


# COMMAND ----------

constructor_final_df_ts = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                                .withColumnRenamed("constructorRef", "constructor_ref") \
                                                .withColumn("ingestion_date", current_timestamp()) \
                                                .withColumn("data_source", lit(v_data_source))


# COMMAND ----------

constructor_final_df = add_ingestion_date(constructor_final_df_ts)

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")
