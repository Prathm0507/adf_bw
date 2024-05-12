# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Aure Data Lake using access keys
# MAGIC
# MAGIC 1.set the spark confg fs.azure.account.key
# MAGIC
# MAGIC 2.list files from demo container
# MAGIC
# MAGIC 3.read data from circuit.csv file

# COMMAND ----------

formula_account_key=dbutils.secrets.get(scope='formula1-scope',key='formula1-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formulaones.dfs.core.windows.net",formula_account_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formulaones.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formulaones.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


