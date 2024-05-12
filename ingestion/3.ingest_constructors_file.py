# Databricks notebook source
# MAGIC %md
# MAGIC Ingest constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1-Read the JSON file using the spark dataframe reader

# COMMAND ----------

constructors_schema="constructorId INT, constructorRef STRING, name STRING,nationality STRING,url STRING"

# COMMAND ----------

constructors_df=spark.read.schema(constructors_schema).json("/mnt/formulaones/raw/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step2 -Drop unwanted columns from the dataframe

# COMMAND ----------

constructor_dropeed_df=constructors_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_final_df = (
    constructor_dropeed_df
    .withColumnRenamed("constructorId", "constructor_id")
    .withColumnRenamed("constructorRef", "constructor_ref")
    .withColumn("ingestion_date", current_timestamp())
    
)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 4- Write output to parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("/mnt/formulaones/processed/constructors")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formulaones/processed/constructors"))

# COMMAND ----------

dbutils.notebook.exit("Success")
