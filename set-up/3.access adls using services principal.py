# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Aure Data Lake using Services Principal
# MAGIC
# MAGIC Steps to follow
# MAGIC
# MAGIC 1.Regeister Azure AD Application/Service Principal
# MAGIC
# MAGIC 2.Gennerate a secret/password for the Application
# MAGIC
# MAGIC 3.Set Spark Confing with App/Client id,Drirectory/ Tenant id & Secret
# MAGIC
# MAGIC 4.Assing role 'srorage blob data contributor' to the data lake

# COMMAND ----------

client_id= dbutils.secrets.get(scope='formula1-scope',key='formua1-app-client-id')
tenant_id=dbutils.secrets.get(scope='formula1-scope',key='formula1-app-tenant-id')
client_secret=dbutils.secrets.get(scope='formula1-scope',key='formula1-app-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formulaones.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formulaones.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formulaones.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formulaones.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formulaones.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formulaones.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formulaones.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


