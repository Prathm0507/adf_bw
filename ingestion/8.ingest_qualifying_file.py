# Databricks notebook source
# MAGIC %md
# MAGIC Ingest qualifying json files

# COMMAND ----------

# MAGIC %md
# MAGIC Step1-Read the JSON file using the spark dataframe API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

qualifying_schema= StructType(fields=[StructField("qualifyId",IntegerType(),False),
                                     
                                     StructField("raceId",IntegerType(),True),
                                      StructField("driverId",IntegerType(),True),
                                      StructField("constructorId",IntegerType(),True),
                                      StructField("number",IntegerType(),True),
                                      StructField("position",IntegerType(),True),
                                      StructField("q1",StringType(),True),
                                      StructField("q2",StringType(),True),
                                      StructField("q3",StringType(),True),            
                                     
                                                            
])

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).option("multiLine", True).json("/mnt/formulaones/raw/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC Step2- Rename columns and add new columns
# MAGIC 1.Rename qualifyingId,driverId,constructorId and raceId
# MAGIC
# MAGIC 2.Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df=qualifying_df.withColumnRenamed("qualifyId","qualify_id").withColumnRenamed("driverId","driver_id").withColumnRenamed("raceId","race_id").withColumnRenamed("constructorId","constructor_id").withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC Step3- Write to output to processed container in parquet format

# COMMAND ----------

final_df.write.mode("overwrite").parquet("mnt/formulaones/processed/qualifying")

# COMMAND ----------

display(spark.read.parquet('/mnt/formulaones/processed/qualifying'))

# COMMAND ----------

dbutils.notebook.exit("Success")
