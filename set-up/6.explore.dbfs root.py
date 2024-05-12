# Databricks notebook source
# MAGIC %md
# MAGIC Explore DBFS Root
# MAGIC
# MAGIC 1.list all the folders in DBFS root 
# MAGIC
# MAGIC 2.Interact with DBFS file Broweser
# MAGIC
# MAGIC 3.Upload file to DBFS Root

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/user'))

# COMMAND ----------

# MAGIC %md
# MAGIC
