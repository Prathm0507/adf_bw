# Databricks notebook source
# MAGIC %md
# MAGIC Ingest pit_stops.json file

# COMMAND ----------

# MAGIC %md
# MAGIC Step1- Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

# COMMAND ----------

pit_stops_schema= StructType(fields=[StructField("raceId",IntegerType(),False),
                                     
                                     StructField("driverId",IntegerType(),True),
                                     StructField("stop",StringType(),True),
                                     StructField("lap",IntegerType(),True),
                                     StructField("time",StringType(),True),
                                     StructField("duration",StringType(),True),
                                     StructField("milliseconds",IntegerType(),True),
                                                                                                 
                                     
                                                            
])

# COMMAND ----------

pit_stops_df=spark.read.schema(pit_stops_schema).option("multiLine",True).json("/mnt/formulaones/raw/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC Step2-RenameColumns and addnew Columns
# MAGIC
# MAGIC 1.Rename driverId and raceId
# MAGIC
# MAGIC 2.Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp 

# COMMAND ----------

final_df=pit_stops_df.withColumnRenamed("driverId","driver_id").withColumnRenamed("raceId","race_id").withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC Step3-Write to output to processed container in parquet format

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/formulaones/processed/pit_stops")

# COMMAND ----------

dbutils.notebook.exit("Success")
