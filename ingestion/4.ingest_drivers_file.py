# Databricks notebook source
# MAGIC %md
# MAGIC Ingest drivers.json file

# COMMAND ----------

# MAGIC %md 
# MAGIC Step 1- Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

# COMMAND ----------

name_schema= StructType(fields=[StructField("forename",StringType(),True),
                                StructField("surname",StringType(),True),
                                                            
])

# COMMAND ----------

drivers_schema= StructType(fields=[StructField("driverId",IntegerType(),False),
                                     StructField("driverRef",StringType(),True),
                                     StructField("number",IntegerType(),True),
                                     StructField("code",StringType(),True),
                                     StructField("name",name_schema),
                                     StructField("dob",DateType(),True),
                                     StructField("nationality",StringType(),True),
                                     StructField("url",StringType(),True),
                                                            
])

# COMMAND ----------

drivers_df=spark.read.schema(drivers_schema).json("/mnt/formulaones/raw/drivers.json")

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2- Rename  columns and add new columns
# MAGIC 1.driverId renamed to driver_id
# MAGIC
# MAGIC 2.driverRef renamed driver_ref
# MAGIC
# MAGIC 3.ingestion date added
# MAGIC
# MAGIC 4.name added with concatenation of forename and surname

# COMMAND ----------

from pyspark.sql.functions import col,concat,current_timestamp,lit

# COMMAND ----------

drivers_with_columns_df= drivers_df.withColumnRenamed("driverId","driver_id").withColumnRenamed("driverRef","driver_ref").withColumn("ingestion_date",current_timestamp()).withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")))

# COMMAND ----------

display(drivers_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3-Drop the unwanted columns
# MAGIC 1.name forename
# MAGIC
# MAGIC 2.name.surname
# MAGIC
# MAGIC 3.url

# COMMAND ----------

drivers_final_df=drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC Step 4- Write to output to procedded container in parquat format

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet("/mnt/formulaones/processed/drivers")

# COMMAND ----------

display(spark.read.parquet("/mnt/formulaones/processed/drivers"))

# COMMAND ----------

dbutils.notebook.exit("Success")
