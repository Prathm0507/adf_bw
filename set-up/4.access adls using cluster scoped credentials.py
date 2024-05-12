# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Aure Data Lake using cluster scoped crendentials 
# MAGIC 1.set the spark confg fs.azure.account.key
# MAGIC
# MAGIC 2.list files from demo container
# MAGIC
# MAGIC 3.read data from circuit.csv file

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formulaones.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formulaones.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


