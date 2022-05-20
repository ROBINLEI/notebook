# Databricks notebook source
tenant_id = dbutils.secrets.get(scope="example-kv-scope",key="SP-TENANT-ID" )
endpoint_str = "https://login.microsoftonline.com/"+ tenant_id  +  "/oauth2/token"
storage_account_name = dbutils.secrets.get(scope="example-kv-scope",key="STORAGE-ACCOUNT-NAME" )
storage_account_access_key = dbutils.secrets.get(scope = "example-kv-scope", key = "DATALAKE-CONN-STRING")
container_name = "hkdata"


# COMMAND ----------

mode = "AK"
if mode == "SP":
    configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id":dbutils.secrets.get(scope="example-kv-scope",key="SP-CLIENT-ID") ,
           "fs.azure.account.oauth2.client.secret":dbutils.secrets.get(scope="example-kv-scope",key="SP-ADLS-SECRET"),
           "fs.azure.account.oauth2.client.endpoint":endpoint_str }
else:
    configs = {"fs.azure.account.key." + storage_account_name + ".dfs.core.windows.net":storage_account_access_key}

mount_point = "/mnt"
# Optionally, you can add <directory-name> to the source URI of your mount point.


# COMMAND ----------



# COMMAND ----------

if not any(mount.mountPoint==mount_point for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(source = "abfss://" + container_name + "@"  + storage_account_name +".dfs.core.windows.net/",mount_point = "/mnt",extra_configs = configs)

# COMMAND ----------

PARAM_SOURCE_AIA_ALL_CONTRACT = "abfss://" + container_name + "@"  + storage_account_name +".dfs.core.windows.net/output/resultnew.csv"

df_AIA_ALL_CONTRACT = spark.read.format("csv").option('header','true').load("/mnt/output/resultnew.csv")

df_AIA_ALL_CONTRACT.show()

# COMMAND ----------

spark.conf.set("fs.azure.account.key."+storage_account_name+".dfs.core.windows.net",storage_account_access_key )

df_AIA_ALL_CONTRACT1 = spark.read.format("csv").option('header','true').load(PARAM_SOURCE_AIA_ALL_CONTRACT)

df_AIA_ALL_CONTRACT1.show()

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