# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Aure Data Lake using SAS Token
# MAGIC 1.set the spark confg for SAS Token
# MAGIC
# MAGIC 2.list files from demo container
# MAGIC
# MAGIC 3.read data from circuit.csv file

# COMMAND ----------

formula1_demo__sas_token=dbutils.secrets.get(scope='formula1-scope',key='formula1-demo-sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formulaones.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formulaones.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formulaones.dfs.core.windows.net",formula1_demo__sas_token) 

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formulaones.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formulaones.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


