# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Aure Data Lake using Services Principal
# MAGIC
# MAGIC Steps to follow
# MAGIC
# MAGIC 1.get client_id,tenant_id and client_secret from key valut
# MAGIC
# MAGIC 2.set spark config with app/client id ,directory/tenant id & secret
# MAGIC
# MAGIC
# MAGIC 3.call file system utility mount to mount the storage
# MAGIC
# MAGIC 4.explore other file system utilities related to mount (list all mounts,unmount)

# COMMAND ----------

client_id= dbutils.secrets.get(scope='formula1-scope',key='formua1-app-client-id')
tenant_id=dbutils.secrets.get(scope='formula1-scope',key='formula1-app-tenant-id')
client_secret=dbutils.secrets.get(scope='formula1-scope',key='formula1-app-client-secret')

# COMMAND ----------

# spark.conf.set("fs.azure.account.auth.type.formulaones.dfs.core.windows.net", "OAuth")
# spark.conf.set("fs.azure.account.oauth.provider.type.formulaones.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# spark.conf.set("fs.azure.account.oauth2.client.id.formulaones.dfs.core.windows.net", client_id)
# spark.conf.set("fs.azure.account.oauth2.client.secret.formulaones.dfs.core.windows.net", client_secret)
# spark.conf.set("fs.azure.account.oauth2.client.endpoint.formulaones.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret":client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}



# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formulaones.dfs.core.windows.net/",
  mount_point = "/mnt/formulaones/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formulaones/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formulaones/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------


