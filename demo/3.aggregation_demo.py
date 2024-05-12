# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC Aggregate function demo

# COMMAND ----------

# MAGIC %md
# MAGIC Built-in Agregate functions

# COMMAND ----------

race_results_df=spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Window Function

# COMMAND ----------

demo_df=race_results_df.filter("race_year in (2019,2020)")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank
driverRankSpec= Window.partitionBy("race_year").orderBy(desc("total_points"))
demo_df.withColumn("rank",rank().over(driverRankSpec))
