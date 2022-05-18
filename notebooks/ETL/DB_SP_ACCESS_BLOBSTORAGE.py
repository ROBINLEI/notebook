# Databricks notebook source
tenant_id = dbutils.secrets.get(scope="example-kv-scope",key="SP-TENANT-ID" )
endpoint_str = "https://login.microsoftonline.com/"+ tenant_id  +  "/oauth2/token"
storage_account_name = dbutils.secrets.get(scope="example-kv-scope",key="STORAGE-ACCOUNT-NAME" )
container_name = "hkdata"


# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id":dbutils.secrets.get(scope="example-kv-scope",key="SP-CLIENT-ID") ,
          "fs.azure.account.oauth2.client.secret":dbutils.secrets.get(scope="example-kv-scope",key="SP-ADLS-SECRET"),
          "fs.azure.account.oauth2.client.endpoint":endpoint_str }

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://" + container_name + "@"  + storage_account_name +".dfs.core.windows.net/",
  mount_point = "/mnt",
  extra_configs = configs)

# COMMAND ----------

PARAM_SOURCE_AIA_ALL_CONTRACT = "abfss://" + container_name + "@"  + storage_account_name +".dfs.core.windows.net/output/resultnew.csv"

df_AIA_ALL_CONTRACT = spark.read.format("csv").option('header','true').load("/mnt/output/resultnew.csv")

df_AIA_ALL_CONTRACT.show()

# COMMAND ----------

dbutils.fs.cp("/mnt/output/resultnew.csv", "/mnt/output/resultnew1.csv")

# COMMAND ----------

dbutils.fs.cp("/mnt/output/resultnew.csv", "/FileStore/new1.csv")

# COMMAND ----------

dbutils.fs.ls("/mnt/output")

# COMMAND ----------

df = spark.read.text("/mnt/output/result.txt")

# COMMAND ----------

df.show()

# COMMAND ----------

mydf = spark.read.option("header","true").csv("/mnt/output/resultnew.csv")
mydf.show()

# COMMAND ----------

mydf1 = spark.read.format("csv").option('header','true').load("/mnt/output/resultnew.csv")
mydf1.show()

# COMMAND ----------

dbutils.fs.unmount("/mnt")

# COMMAND ----------

print("Program End")