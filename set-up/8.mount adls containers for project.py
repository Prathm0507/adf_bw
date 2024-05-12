# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake Containers for the Project

# COMMAND ----------

def mount_adls(storage_account_name,container_name):
    #get secrets from key valut
    client_id= dbutils.secrets.get(scope='formula1-scope',key='formua1-app-client-id')
    tenant_id=dbutils.secrets.get(scope='formula1-scope',key='formula1-app-tenant-id')
    client_secret=dbutils.secrets.get(scope='formula1-scope',key='formula1-app-client-secret')

    #set spark configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret":client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    

    #mount the storage account container
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)
    
    display(dbutils.fs.mounts())
    

 

# COMMAND ----------

# MAGIC %md
# MAGIC Mount Raw Container

# COMMAND ----------

mount_adls('formulaones','raw')

# COMMAND ----------

mount_adls('formulaones','processed')

# COMMAND ----------

mount_adls('formulaones','presentation')

# COMMAND ----------

 client_id= dbutils.secrets.get(scope='formula1-scope',key='formua1-app-client-id')
tenant_id=dbutils.secrets.get(scope='formula1-scope',key='formula1-app-tenant-id')
client_secret=dbutils.secrets.get(scope='formula1-scope',key='formula1-app-client-secret')

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


