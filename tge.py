# Databricks notebook source


# COMMAND ----------

BAVCJ424BQ4B31HV7XZ5A84B

# COMMAND ----------

# DBTITLE 1,working scenario
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Gmail sender credentials
sender_email = "twitterstallion2192@gmail.com"
app_password = "omwq vknl giph pygv"  # App Password, not your Gmail password omwq vknl giph pygv
receiver_email = "tjeshwanth17@gmail.com"

# Email content
subject = "Test Email via Gmail SMTP"
body = "This is a test email sent from a local Python script using Gmail's SMTP server."

# Create email message
message = MIMEMultipart()
message["From"] = sender_email
message["To"] = receiver_email
message["Subject"] = subject
message.attach(MIMEText(body, "plain"))

try:
    print("Connecting to Gmail SMTP server...")
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.set_debuglevel(1)  # Shows full SMTP logs
        server.starttls()
        server.login(sender_email, app_password)
        server.send_message(message)
    print("✅ Email sent successfully!")
except Exception as e:
    print(f"❌ Failed to send email: {e}")

# COMMAND ----------

dbutils.fs.put("/mnt/bronze/jerry/thing/testfilefinalcloud.csv1", "This is a test file hlo hi hw r u jesw and abhiram.")

# COMMAND ----------

csv_content = "name,message\njesw,\"hlo hi hw r u\"\nabhiram,\"hlo hi hw r u\""
dbutils.fs.put("/mnt/bronze/jerry/thing/dummy_src_1234.csv", csv_content, overwrite=True)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# --- 🌐 CONFIGURE STORAGE ACCESS ---
storage_account = "mystorage.dfs.core.windows.net"
app_id = dbutils.secrets.get("kv_scope", "sp_client_id")
app_secret = dbutils.secrets.get("kv_scope", "sp_client_secret")
tenant_id = dbutils.secrets.get("kv_scope", "sp_tenant_id")

# Spark config for ADLS Gen2
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}", app_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}", app_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}",
               f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# --- 📂 AUTO LOADER CONFIG ---
input_path = f"abfss://raw@mystorage.dfs.core.windows.net/customer_data/"
checkpoint_loc = f"/mnt/checkpoints/customer_data/checkpoint/"
schema_loc = f"/mnt/checkpoints/customer_data/schema/"

autoloader_opts = {
  "cloudFiles.format": "json",                # input format
  "cloudFiles.schemaLocation": schema_loc,    # infer & evolve schema
  "cloudFiles.useNotifications": "true",      # faster Event Grid ingestion :contentReference[oaicite:3]{index=3}
  "cloudFiles.inferColumnTypes": "true",
  "includeExistingFiles": "true"
}

# --- ⚙️ BUILD STREAMING DATAFRAME ---
df_stream = (spark.readStream
  .format("cloudFiles")
  .options(**autoloader_opts)
  .load(input_path)
)

# Optional transformations
df_processed = df_stream.withColumn("ingested_at", current_timestamp())

# --- 💾 WRITE STREAM TO DELTA ---
output_path = "/mnt/bronze/customer_data/"

streaming_query = (df_processed.writeStream
  .format("delta")
  .option("checkpointLocation", checkpoint_loc)
  .outputMode("append")
  .trigger(availableNow=True)  # batch-run current snapshot :contentReference[oaicite:4]{index=4}
  .start(output_path)
)

display(streaming_query)  # monitor in notebook


# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# DBTITLE 1,working scenario
# tenant_id="60419507-249c-412c-bf2e-a187855acd2f",  # Ensure this is in the correct format
# app_id="15e6792e-c481-462a-bc6b-c402732dcc39",
# app_secret="Qbp8Q~Y9~h18-dGi5iz38hgeRwBS1oxfWAXn2c0s",
# storage_account = "stallion2192.dfs.core.windows.net"
# Define storage account and secrets
storage_account = "stallion2192.dfs.core.windows.net"
# app_id = dbutils.secrets.get(scope="keyclinet", key="clinetid")
# app_secret = dbutils.secrets.get(scope="newsk", key="new")
# tenant_id = dbutils.secrets.get(scope="keytenant", key="tenantid")

# Set Spark configs (FIXED string formatting using f-strings)
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}", 
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}", "15e6792e-c481-462a-bc6b-c402732dcc39")
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}", "Qbp8Q~Y9~h18-dGi5iz38hgeRwBS1oxfWAXn2c0s")
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}", 
               f"https://login.microsoftonline.com/60419507-249c-412c-bf2e-a187855acd2f/oauth2/token")

# Try accessing the container
display(dbutils.fs.ls("abfss://jerry@stallion2192.dfs.core.windows.net/thing/"))

# COMMAND ----------

dbutils.fs.ls("abfss://jerry@stallion2192.dfs.core.windows.net/thing")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# --- 🌐 CONFIGURE STORAGE ACCESS ---
storage_account = "stallion2192.dfs.core.windows.net"
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}", 
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}", "15e6792e-c481-462a-bc6b-c402732dcc39")
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}", "Qbp8Q~Y9~h18-dGi5iz38hgeRwBS1oxfWAXn2c0s")
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}", 
               f"https://login.microsoftonline.com/60419507-249c-412c-bf2e-a187855acd2f/oauth2/token")

# --- 📂 AUTO LOADER CONFIG ---
input_path = f"abfss://jerry@stallion2192.dfs.core.windows.net/thing/"
checkpoint_loc = f"/mnt/bronze/jerry/checkpoint/"
schema_loc = f"/mnt/bronze/jerry/checkpoint/schema/"
autoloader_opts = {
  "cloudFiles.format": "json",
  "cloudFiles.schemaLocation": schema_loc,
  "cloudFiles.useNotifications": "true",
  "cloudFiles.inferColumnTypes": "true",
  "includeExistingFiles": "true",
  "cloudFiles.maxFilesPerTrigger": "1000"
}


# COMMAND ----------

# MAGIC %sql CREATE TABLE stallion.learning.orders (
# MAGIC   order_id        BIGINT,
# MAGIC   customer_id     BIGINT,
# MAGIC   order_status    STRING,
# MAGIC   order_date      DATE,
# MAGIC   required_date   DATE,
# MAGIC   shipped_date    DATE,
# MAGIC   store_id        BIGINT,
# MAGIC   staff_id        BIGINT,
# MAGIC   total_amount    DECIMAL(18,2)
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW stallion.learning.orders

# COMMAND ----------

dbutils.fs.put("/mnt/bronze/jerry/thing/testfilefinalcloud.csv", "This is a test file hlo hi hw r u jesw and abhiram.", True)

# COMMAND ----------

# Parameters - move these to dbutils.secrets in production
subscription_id = "c83eea8e-c0d4-4483-bbad-3a30fa6043e9"
client_id = "15e6792e-c481-462a-bc6b-c402732dcc39"
client_secret = "Qbp8Q~Y9~h18-dGi5iz38hgeRwBS1oxfWAXn2c0s"
tenant_id = "60419507-249c-412c-bf2e-a187855acd2f"

# Paths
data_source_path = "abfss://jerry@stallion2192.dfs.core.windows.net/thing/"
checkpoint_path = "/mnt/bronze/jerry/_checkpoint"
schema_loc = f"{checkpoint_path}/schema"
target_table_name = "stallion.learning.orders"
source_format = "csv"
# Set Spark configs (FIXED string formatting using f-strings)
storage_account = "stallion2192.dfs.core.windows.net"
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}", 
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}", "15e6792e-c481-462a-bc6b-c402732dcc39")
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}", "Qbp8Q~Y9~h18-dGi5iz38hgeRwBS1oxfWAXn2c0s")
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}", 
               f"https://login.microsoftonline.com/60419507-249c-412c-bf2e-a187855acd2f/oauth2/token")

# Auto Loader function
def autoload_to_delta(data_source, source_format, table_name, checkpoint_path, schema_path):
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", source_format)
            .option("cloudFiles.schemaLocation", schema_path)
            .option("cloudFiles.useNotifications", "true")
            .option("fs.azure.account.key.{storage_account}", <account_key>)
            .option("cloudFiles.tenantId", tenant_id)
            .option("cloudFiles.clientId", client_id)
            .option("cloudFiles.clientSecret", client_secret)
            .load(data_source)
            .writeStream
            .trigger(availableNow=True)  # for batch-like behavior
            .format("delta")
            .queryName("autoloader_orders_csv")
            .option("checkpointLocation", checkpoint_path)
            .option("mergeSchema", "true")
            .toTable(table_name)
    )

# Main block to run
if __name__ == "__main__":
    query = autoload_to_delta(data_source_path, source_format, target_table_name, checkpoint_path, schema_loc)
    query.awaitTermination()


# COMMAND ----------

client_id = dbutils.secrets.get(scope="keyclinet", key="clinetid")
client_secret = dbutils.secrets.get(scope="newsk", key="new")
tenant_id=dbutils.secrets.get(scope="keytenant", key="tenantid")  # Replace with actual tenant ID string

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# Optional: Unmount first to avoid duplicate mounts
mount_point = "/mnt/yourmount/praneeth"
if any(m.mountPoint == mount_point for m in dbutils.fs.mounts()):
    dbutils.fs.unmount(mount_point)

# Now perform the mount
dbutils.fs.mount(
    source="abfss://jerry@stallion2192.dfs.core.windows.net/",
    mount_point=mount_point,
    extra_configs=configs
)
ExecutionError: An error occurred while calling o438.mount.
: java.lang.NullPointerException
	at shaded.databricks.azurebfs.org.apache.hadoop.fs.azurebfs.oauth2.AzureADAuthenticator.getTokenCall(AzureADAuthenticator.java:320)
	at shaded.databricks.azurebfs.org.apache.hadoop.fs.azurebfs.oauth2.AzureADAuthenticator.getTokenCall(AzureADAuthenticator.java:287)
	at shaded.databricks.azurebfs.org.apache.hadoop.fs.azurebfs.oauth2.AzureADAuthenticator.getTokenUsingClientCreds(AzureADAuthenticator.java:110)
	at com.databricks.backend.daemon.dbutils.DBUtilsCore.verifyAzureOAuth(DBUtilsCore.scala:1287)
	at com.databricks.backend.daemon.dbutils.DBUtilsCore.verifyAzureFileSystem(DBUtilsCore.scala:1299)
	at com.databricks.backend.daemon.dbutils.DBUtilsCore.createOrUpdateMount(DBUtilsCore.scala:1183)
	at com.databricks.backend.daemon.dbutils.DBUtilsCore.$anonfun$mount$1(DBUtilsCore.scala:1231)
	at com.databricks.logging.UsageLogging.$anonfun$recordOperation$1(UsageLogging.scala:528)
	at com.databricks.logging.UsageLogging.executeThunkAndCaptureResultTags$1(UsageLogging.scala:633)
	at com.databricks.logging.UsageLogging.$anonfun$recordOperationWithResultTags$4(UsageLogging.scala:656)
	at com.databricks.logging.AttributionContextTracing.$anonfun$withAttributionContext$1(AttributionContextTracing.scala:48)
	at com.databricks.logging.AttributionContext$.$anonfun$withValue$1(AttributionContext.scala:276)
	at scala.util.DynamicVariable.withValue(DynamicVariable.scala:62)
	at com.databricks.logging.AttributionContext$.withValue(AttributionContext.scala:272)
	at com.databricks.logging.AttributionContextTracing.withAttributionContext(AttributionContextTracing.scala:46)
	at com.databricks.logging.AttributionContextTracing.withAttributionContext$(AttributionContextTracing.scala:43)
	at com.databricks.backend.daemon.dbutils.FSUtils.withAttributionContext(DBUtilsCore.scala:76)
	at com.databricks.logging.AttributionContextTracing.withAttributionTags(AttributionContextTracing.scala:95)
	at com.databricks.logging.AttributionContextTracing.withAttributionTags$(AttributionContextTracing.scala:76)
	at com.databricks.backend.daemon.dbutils.FSUtils.withAttributionTags(DBUtilsCore.scala:76)
	at com.databricks.logging.UsageLogging.recordOperationWithResultTags(UsageLogging.scala:628)
	at com.databricks.logging.UsageLogging.recordOperationWithResultTags$(UsageLogging.scala:537)
	at com.databricks.backend.daemon.dbutils.FSUtils.recordOperationWithResultTags(DBUtilsCore.scala:76)
	at com.databricks.logging.UsageLogging.recordOperation(UsageLogging.scala:529)
	at com.databricks.logging.UsageLogging.recordOperation$(UsageLogging.scala:495)
	at com.databricks.backend.daemon.dbutils.FSUtils.recordOperation(DBUtilsCore.scala:76)
	at com.databricks.backend.daemon.dbutils.FSUtils.recordDbutilsFsOp(DBUtilsCore.scala:140)
	at com.databricks.backend.daemon.dbutils.DBUtilsCore.mount(DBUtilsCore.scala:1225)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:498)
	at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
	at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:397)
	at py4j.Gateway.invoke(Gateway.java:306)
	at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
	at py4j.commands.CallCommand.execute(CallCommand.java:79)
	at py4j.ClientServerConnection.waitForCommands(ClientServerConnection.java:199)
	at py4j.ClientServerConnection.run(ClientServerConnection.java:119)
	at java.lang.Thread.run(Thread.java:750)

# COMMAND ----------

# List all secret scopes
scopes = dbutils.secrets.listScopes()
display(scopes) 
# Retrieve a secret value from a secret scope
username = dbutils.secrets.get(scope="keyscopetest", key="storageaccount")
storage_account_key = dbutils.secrets.get(scope="testrg", key="testresourcegroup")
# display(dbutils.secrets.list("keyscopetest"))
print(username)
dbutils.secrets.get(scope="keyclinet", key="clinetid")
dbutils.secrets.get(scope="keytenant", key="tenantid")
dbutils.secrets.get(scope="keyclinet", key="clinetsecret")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.put("/mnt/bronze/jerry/thing/testfilefinalcloud.csv", "This is a test file hlo hi hw r u jesw and abhiram.")

# COMMAND ----------

Storage Account Contributor 
Storage Queue Data Contributor
stor age blob data contributor

# COMMAND ----------

# List all secret scopes
scopes = dbutils.secrets.listScopes()
display(scopes) 
# Retrieve a secret value from a secret scope
username = dbutils.secrets.get(scope="keyscopetest", key="storageaccount")
storage_account_key = dbutils.secrets.get(scope="testrg", key="testresourcegroup")
# mounting storage account
container_name = "jerry"
mount_point = "/mnt/bronze/jerry"
storage_account_name = "stallion2192"

# Retrieve the storage account key from the secret scope
storage_account_key = dbutils.secrets.get(scope="newst", key="sa2025")

# Unmount if already mounted
if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
    dbutils.fs.unmount(mount_point)

# Mount the container using the correct key variable
dbutils.fs.mount(
    source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/",
    mount_point=mount_point,
    extra_configs={
        f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_key
    }
)

# COMMAND ----------

dbutils.library.restartPython()


# COMMAND ----------

from azure.identity import DefaultAzureCredential
import requests
import json
client_id=dbutils.secrets.get(scope="keyclinet", key="clinetid")
tenant_id=dbutils.secrets.get(scope="keytenant", key="tenantid")
clinet=dbutils.secrets.get(scope="clientup", key="clinetsecret")
from azure.identity import ClientSecretCredential
credential = ClientSecretCredential(tenant_id, client_id, clinet)
token = credential.get_token("https://api.loganalytics.io/.default").token

# COMMAND ----------

from azure.identity import ClientSecretCredential

credential = ClientSecretCredential(
    tenant_id="60419507-249c-412c-bf2e-a187855acd2f",  # Ensure this is in the correct format
    client_id="15e6792e-c481-462a-bc6b-c402732dcc39",
    client_secret="Qbp8Q~Y9~h18-dGi5iz38hgeRwBS1oxfWAXn2c0s",
    authority="https://login.microsoftonline.com",  # Use custom authority if needed
    additionally_allowed_tenants=["*"]  # Allow acquiring tokens for any tenant
)

token = credential.get_token("https://api.loganalytics.io/.default").token


# COMMAND ----------

import requests

tenant_id="60419507-249c-412c-bf2e-a187855acd2f",  # Ensure this is in the correct format
client_id="15e6792e-c481-462a-bc6b-c402732dcc39",
client_secret="Qbp8Q~Y9~h18-dGi5iz38hgeRwBS1oxfWAXn2c0s",

token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

payload = {
    'client_id': client_id,
    'scope': 'https://logic.azure.com/.default',
    'client_secret': client_secret,
    'grant_type': 'client_credentials'
}

response = requests.post(token_url, data=payload)
token_response = response.json()

access_token = token_response.get('access_token')

print("Access Token:", access_token)


# COMMAND ----------


# from azure.identity import ClientSecretCredential

# credential = ClientSecretCredential(
#     client_id=dbutils.secrets.get(scope="keyclinet", key="clinetid"),
#     tenant_id=dbutils.secrets.get(scope="keytenant", key="tenantid"),
#     client_secret=dbutils.secrets.get(scope="clientup", key="clinetsecret"),
#     authority="https://login.microsoftonline.com",  # Use custom authority if needed
#     additionally_allowed_tenants=["*"]  # Allow acquiring tokens for any tenant
# )

# token = credential.get_token("https://api.loganalytics.io/.default").token


# COMMAND ----------

workspace_id = "4973690c-ea08-4032-ba0b-f7b47bcde09b"
query = "AzureActivity | take 10"

url = f"https://api.loganalytics.io/v1/workspaces/{workspace_id}/query"
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}
body = {
    "query": query
}

response = requests.post(url, headers=headers, json=body)
data = response.json()


# COMMAND ----------

response

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_utc_timestamp

display(spark.range(2).select(to_utc_timestamp(current_timestamp(), "UTC"), current_timestamp()))
# 2025-07-08T08:11:59.467+00:00

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DateType

orders_data = [
    (1, 201, '2025-06-23'),
    (2, 202, '2025-06-23'),
    (3, 203, '2025-06-24'),
    (4, 204, '2025-06-25'),
    (5, 205, '2025-06-26'),
    (6, 206, '2025-06-26'),
    (7, 207, '2025-06-28'),
    (8, 208, '2025-06-28'),
    (9, 209, '2025-06-28'),
    (10, 210, '2025-06-29'),
    (11, 211, '2025-06-29'),
    (12, 212, '2025-06-30'),
    (13, 213, '2025-07-01'),
    (14, 214, '2025-07-01'),
    (15, 215, '2025-07-02'),
    (16, 216, '2025-07-02'),
    (17, 217, '2025-07-02'),
    (18, 218, '2025-07-03'),
    (19, 219, '2025-07-04'),
    (20, 220, '2025-07-05')
]

orders_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("order_date", DateType(), False)
])

from pyspark.sql import Row
from datetime import datetime

orders_df = spark.createDataFrame(
    [Row(order_id=o[0], customer_id=o[1], order_date=datetime.strptime(o[2], "%Y-%m-%d").date()) for o in orders_data],
    schema=orders_schema
)
from pyspark.sql.functions import *
from pyspark.sql.types import *
# 1. Find the total number of orders placed on that date
# 2. Calculate a 7-day rolling average of daily orders — which means:# For each date, take the average number of orders from that day and the previous 6 days (if available).
from pyspark.sql.functions import avg, col
# First, create a DataFrame with the count of orders per date
orders_count_df = orders_df.groupBy("order_date").agg(count("order_id").alias("total_orders"))
from pyspark.sql.window import *
# Then, define the window
window_spec = Window.orderBy("order_date").rowsBetween(-6, 0)

# Calculate the rolling average
df2 = orders_count_df.withColumn("n", avg(col("total_orders")).over(window_spec))

display(df2)


# COMMAND ----------

df=spark.range(10000000).toDF("input_col")
df.repartition(100).agg(sum("input_col").alias("st")).show()

# COMMAND ----------


# Ankur Warikoo, an influential figure in Indian social media, shares a guideline in one of his videos called the 20-6-20 rule for determining whether one can afford to buy a phone or not. The rule for affordability entails three conditions:
# 1. Having enough savings to cover a 20 percent down payment.
# 2. Utilizing a maximum 6-month EMI plan (no-cost) for the remaining cost.
# 3. Monthly EMI should not exceed 20 percent of one's monthly salary.
# Given the salary and savings of various users, along with data on phone costs, the task is to write an SQL to generate a list of phones (comma-separated) that each user can afford based on these criteria, display the output in ascending order of the user name.
users_data = [
    ('Rahul', 40000, 15000),
    ('Vivek', 70000, 10000)
]

phones_data = [
    ('iphone-12', 60000),
    ('oneplus-12', 50000),
    ('iphone-14', 70000)
]
from pyspark.sql.functions import *
from pyspark.sql.types import *
users_schema = StructType([
    StructField("user_name", StringType(), False),
    StructField("monthly_salary", IntegerType(), False),
    StructField("savings", IntegerType(), False)
])

phones_schema = StructType([
    StructField("phone_name", StringType(), False),
    StructField("cost", IntegerType(), False)
])

users_df = spark.createDataFrame(users_data, schema=users_schema)
phones_df = spark.createDataFrame(phones_data, schema=phones_schema)

df1=phones_df.withColumn("dp", round(col("cost")*0.20))\
                .withColumn("emi", round(col("cost")*0.80/6))
df2=users_df.join(df1, how="cross")\
    .filter((col("dp")<=col("savings")) & 
    (col("emi")<=col("monthly_salary")*0.20))\
        .groupBy("user_name").agg(collect_list("phone_name").alias("phone"))
df2.display()

# COMMAND ----------

# DBTITLE 1,data quality rules
# Databricks notebook source
# MAGIC %md ###Common Functions for FPNA workflows
# MAGIC Overview :
# MAGIC This notebook will include common functions used across different tracks like reading,writing data to Table
# MAGIC
# MAGIC ###History
# MAGIC
# MAGIC | Date |  By | Change Reason |
# MAGIC |:----:|--------------|--------|
# MAGIC |06-15-2022 | Nirmala Jayakumar |Initial Setup |
# MAGIC |06-28-2022 | Suri |DQ Framework |
# MAGIC |06-30-2022 | Nirmala Jayakumar | Synapse Write |
# MAGIC |07-27-2022 | Harshini Devathi | Audit block and ETL functions |
# MAGIC |09-14-2022 | Harshini Devathi | SalesGeofunction |
# MAGIC |10-06-2022 | Harshini Devathi | Added Snapshot function |
# MAGIC |10-25-2022 | Anbarasi Jothi | Added DB parameters |
# MAGIC |12-09-2022 | Deepak Sahoo | Added Function for Process Log |
# MAGIC |28-12-2022 | Jeewan Joshi | Updated SalesGeofunction Select Query logic for XTNObjectCharacteristicValueData|
# MAGIC |01-03-2023 | Deepak Sahoo | Data Retention Implementation using SCD Type-2 |
# MAGIC |01-09-2023 | Harshini Devathi | Orchestration functions for handshake with TM1 team |
# MAGIC |04-24-2023 | Deepak Sahoo | Added Function for Deriving Data Load Stats from Delta Table |
# MAGIC |05-02-2023| Deepak & Zhicheng | Enhance load_data_with_history by capturing statistic & table details into Log Analytics |
# MAGIC |05-09-2023 | Vladislav Vlasov | Added function to apply date filter |
# MAGIC |05-22-2023 | Otacilio Filho | Added function to clean non-breaking spaces |
# MAGIC |08-17-2023 | Otacilio Filho | Upgrade snapshot_creation to enable schema drift |
# MAGIC |08-21-2023 | Deepak Sahoo | Enhanced "SCDType2DataLoad" Function for concurrent execution & restore table to previous version incase of failure |
# MAGIC |08-22-2023 | Otacilio Filho | Change nb_runs to return results to enable parse the results |
# MAGIC |06-09-2023 | Vladislav Vlasov | Add data foundation bronze masterdata and finance schemas |
# MAGIC |08-09-2023 | Amrit Pratihar | Add data foundation bronze sales and procurement schemas |
# MAGIC |09-12-2023 | Deepak Sahoo | Added Data Foundation Prod Silver Layer Schemas |
# MAGIC |20-09-2023 | Vladislav Vlasov | Add function make_tiger_skus |
# MAGIC |24-01-2024 | Amrit Pratihar | Updated "SCDType2DataLoad" to validate against SourceDF |
# MAGIC |02-26-2024 | Brandon Wallace | Added Key/Value Pair for extract_input_details |
# MAGIC |03-19-2024 | Deepak Sahoo | Added Key/Value Pair for Foundation Databases: manufacturing_restricted & manufacturing_public |
# MAGIC |05-06-2024 | Amrit Pratihar | Added variables for AI Databases: finance_fpna_ai_gold |
# MAGIC |07-31-2024 | Tejaswani B A | Added Version Comparison Function: getVersionDifferenceCount|
# MAGIC |10-17-2024 | Brandon Wallace | Unity Catalog Changes |
# MAGIC |10-31-2024 | Brandon Wallace | Unity Catalog Stored Procedure Execution |
# MAGIC |04-25-2024 | Kshitij Agrawal | Added funtions to update DataTransmission_control_table and WorkflowStatusLogs tables in DBSQL |
# MAGIC |05-19-2025 | Fernando Sanchez |Added TPM - FTS email functions |
# MAGIC |06-20-2025 | Arun Pradhan |Added functions to check the TM1 status before the gold table data load, WF status update in data transmission contol table |

# COMMAND ----------

# MAGIC %md #### Read Block

# COMMAND ----------

# MAGIC %md #####Deprecated soon - read_table_to_sparkdf

# COMMAND ----------

# DBTITLE 1,Reading Table data to Dataframe
def read_table_to_sparkdf(dbname,tblname):
  try:
    df = spark.sql("select * from {0}.{1}".format(dbname,tblname))
    return df
  except Exception as error_msg:
    print("Error while reading table to spark dataframe {0}".format(error_msg))
    nbpath = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    error_string = str(error_msg)
    spark.sql(f"""Insert into {audit_db}.softexceptionlog values ('{nbpath}','MosaicCommonUtilities','read_table_to_sparkdf',from_utc_timestamp(current_timestamp,'US/Eastern'),'{market}',"{error_string}");""")

# COMMAND ----------

# MAGIC %md ####Rename Column Names

# COMMAND ----------

# MAGIC %md #####Deprecated soon - rename_columns

# COMMAND ----------

# DBTITLE 1,Rename Column Names in DataFrame with Dictionary
def rename_columns(df,columns):
  try:
    if isinstance(columns, dict):
        return df.select(*[col(col_name).alias(columns.get(col_name)) for col_name in columns.keys()])
#         return df.select(*[col(col_name).alias(columns.get(col_name)) for col_name in df.columns])
    else:
        raise ValueError("'columns' should be a dict, like {'old_name_1':'new_name_1', 'old_name_2':'new_name_2'}")
  except Exception as e:
    raise e

# COMMAND ----------

# MAGIC %md #### Audit Block

# COMMAND ----------

def auditLog(status_cd, msg_desc, audit_config, post_log = 'yes', check_target_table_count ='no'):
  audit_entry={**audit_config, 'status_cd':status_cd, 'msg_desc':msg_desc,'end_tms':str(datetime.now()),'post_log':post_log,'check_target_table_count':check_target_table_count}
  au.log_audit_data(**audit_entry)

# COMMAND ----------

# MAGIC %md ### Parallel run libraries & functions

# COMMAND ----------

# DBTITLE 1,NotebookData class
from concurrent.futures import ThreadPoolExecutor

class NotebookData:
  def __init__(self, path, timeout, parameters=None, retry=0):
    self.path = path
    self.timeout = timeout
    self.parameters = parameters
    self.retry = retry

  def submitNotebook(notebook):
    print("Running notebook %s" % notebook.path)
    try:
      if (notebook.parameters):
        return dbutils.notebook.run(notebook.path, notebook.timeout, notebook.parameters)
      else:
        return dbutils.notebook.run(notebook.path, notebook.timeout)
    except Exception:
       if notebook.retry < 1:
        raise
    print("Retrying notebook %s" % notebook.path)
    notebook.retry = notebook.retry - 1
    submitNotebook(notebook)

# COMMAND ----------

def parallelNotebooks(notebooks, numInParallel):
   # If you create too many notebooks in parallel the driver may crash when you submit all of the jobs at once. 
   # This code limits the number of parallel notebooks.
  try:
    with ThreadPoolExecutor(max_workers=numInParallel) as ec:
      return [ec.submit(NotebookData.submitNotebook, notebook) for notebook in notebooks]
  except Exception:
    raise


# COMMAND ----------

# MAGIC %md #### Running notebooks in parallel and in series

# COMMAND ----------

def nb_runs(notebooks):
  """1. Input parameters  
    a. notebooks - list of notebooks of type NotebookData for using the parallelNotebooks function. Provided as a list in the following format   
            notebooks = [NotebookData(NB_1_path, timeout_par, params1), NotebookData(NB_2_path, timeout_par, params2)]. timeout par is currently used as zero and is defined in MosaicCommonParameterSetup. params is passed as a dictionary.
    2. Purpose  
    a. This function can be used for both series and parallel runs
    b. Used along with parallelNotebooks function and NotebookData class"""
  
  nb_nums = len(notebooks)
  res = parallelNotebooks(notebooks, nb_nums)
  result = [i.result(timeout = timeout_par) for i in res] # This is a blocking call.
  print(result)
  print(res)
  return result

# COMMAND ----------

# MAGIC %md ### Useful ETL functions

# COMMAND ----------

# MAGIC %md #### Transactional parameter extraction

# COMMAND ----------

# def YearPeriodFunc(transaction_params):
#   """This function is used to get the transaction parameters in YYYYppp, YYYYpp format for transactional workflows. Pass transaction parameters as it comes from the ADF. 
   
#   This function populates the default values in the event no transaction parameters are passed from the ADF"""
  
#   YearPeriod = list([x.strip() for x in list(transaction_params.split(',')) if x not in ('', ' ')])
  
#   if YearPeriod == []:
#     import datetime
#     import math
    
#     my_date = datetime.date.today()
#     year, week_num, day_of_week = my_date.isocalendar()
#     week_num = 5 if week_num == 53 else week_num
#     period = (week_num / 4) + 1

#     period_rounded = str(int(math.modf(period)[1]))
#     zerostring = '' if len(period_rounded) == 2 else '0'
#     period_zeroed = f"{zerostring}{period_rounded}"
#     YearPeriod = [f"{year}{period_zeroed}"]

#     if math.modf(period)[0] == 0.25:

#       prev_year = str(year-1)
#       prev_yearperiod = f"{prev_year}013" if period_zeroed == '001' else str(int(YearPeriod[0]) - 1)
#       YearPeriod.append(prev_yearperiod)

#   YearPeriod3Digits = [x[0:4] + '0' + x[4:] for x in YearPeriod]
  
#   print("YearPeriod values with 3 digits: ", YearPeriod3Digits)
#   print("YearPeriod values with 2 digits: ", YearPeriod)
  
#   return YearPeriod3Digits, YearPeriod

# COMMAND ----------

from datetime import datetime


def get_yearperiodmonth_calendarmonth(transaction_params, period_close_day=7, calendar_id='12'):
    """
    This function is used to get the transaction parameters in 
    YYYYppp, YYYYpp format for transactional workflows.
    
    Pass transaction parameters as it comes from the ADF.
   
    This function populates value based on the current date if no transaction 
    parameters are passed from the ADF.

    This function is based on calendar month. 
    
    If no transaction parameter passed and the current date day is less than `period_close_day` parameter,
    then return previous month/period.

    Args:
      transaction_params: A comma separated string with YYYYppp or YYYYpp values
      period_close_day: A digit of a period close month working day number

    Returns:
      Two arrays - yearmonth (YYYYpp) and yearperiod (YYYYppp).

    Raises:
      -
    """
    load_year_month = []
    load_year_period = []

    if transaction_params is None or transaction_params.strip() == '':
        current_date = datetime.today()
        current_year = str(current_date.year)
        current_month = '{:02d}'.format(current_date.month)

        # Add current month
        load_year_month.append(current_year + current_month)
        load_year_period.append(current_year + '0' + current_month)

        # Check for period close and add pervious month
        df_calendar_period_close_days = spark.sql(f"""
            SELECT
                DAY(Date) AS Day
            FROM {silver_masterdata_public_db}.calendardate 
            WHERE 
                CalendarId = '{calendar_id}' 
                AND Date LIKE '{current_year}-{current_month}%' 
                AND DayOfWeekId NOT IN (1, 7) 
            ORDER BY Date 
            LIMIT {period_close_day}
        """)

        period_closing_days = df_calendar_period_close_days.toPandas()['Day'].tolist()

        if current_date.day <= period_closing_days[-1]:
            previous_per = pd.to_datetime(current_date) + pd.DateOffset(months=-1)
            previous_per = previous_per.date()

            load_year_month.append(str(previous_per.year) + '{:02d}'.format(previous_per.month))
            load_year_period.append(str(previous_per.year) + '0' + '{:02d}'.format(previous_per.month))
    else:
        load_year_month = list([x.strip()[:4] + x.strip()[-2:] for x in list(transaction_params.split(',')) if x not in ('', ' ')])
        load_year_period = list([x[:4] + '0' + x[-2:] for x in load_year_month])
        
    return sorted(load_year_period), sorted(load_year_month)

# COMMAND ----------

def YearPeriodFunc(transaction_params):
  """This function is used to get the transaction parameters in YYYYppp, YYYYpp format for transactional workflows. Pass transaction parameters as it comes from the ADF. 
   
  This function populates the default values in the event no transaction parameters are passed from the ADF"""
  
  YearPeriod = list([x.strip() for x in list(transaction_params.split(',')) if x not in ('', ' ')])
  
  if YearPeriod == []:
    dt = spark.sql(f"""select distinct case
                   when substr(CalendarWeekName,2,2) == "53"
                   then "5"
                   else int(substr(CalendarWeekName,2,2)%4)
                   end as WeekNumber,
                   right(CalendarMonthId,2) as Period,
                   CalendarYearId
                   from {silver_masterdata_public_db}.CalendarWeek
                   where cast(current_timestamp() as date) >= CalendarWeekStartDate
                   and cast(current_timestamp() as date) <= CalendarWeekEndDate
                   and CalendarId = '11'
                   """).collect()
    for i in range(len(dt)):
      YearPeriod.append(str(dt[i]['CalendarYearId']) + str(dt[i]['Period']))

  YearPeriod3Digits = [x[0:4] + '0' + x[4:] for x in YearPeriod]
  
  print("YearPeriod values with 3 digits: ", YearPeriod3Digits)
  print("YearPeriod values with 2 digits: ", YearPeriod)
  
  return YearPeriod3Digits, YearPeriod

# COMMAND ----------

# MAGIC %md ####Input details processing

# COMMAND ----------

def get_logical_mapping(logical_key):
    """
    This function returns the underlying UC schema for a given logical database key and table name

    Parameters:
        - str: logical_key: Formed by concatenating the database key and table name
            (e.g., "masterdata.xtnlocationexternal")
    Returns:
        - str: The underlying UC schema and table name
            (e.g., "uc_catalog.fdn_location_view.xtnlocationexternal")
        - str: If no mapping is found, default HMS mapping will be used
    """
    query = f"SELECT UnderlyingUCSchema FROM {audit_db}.unitycataloglogicalmapping WHERE LOWER(CONCAT(MappingKey, '.', TableName)) = LOWER('{logical_key}') AND ActiveFlag = 'Y'"

    logical_mapping_df = spark.sql(query)

    if logical_mapping_df.count() != 0:
        underlying_schema = logical_mapping_df.collect()[0][0]
        lowerEnvCopy_key = logical_key.split('.')[0].strip().lower()

        if lowerEnvCopy_key.endswith('p2d'):
            logical_mapping_str = f"uc_p2d_snt_fdn_01.{underlying_schema}"
        # Under UC, fdn p2pp is deprecated for direct PreProd - Prod Catalog Access
        elif lowerEnvCopy_key.endswith('p2pp'):
            logical_mapping_str = f"uc_prod_snt_fdn_01.{underlying_schema}"
        else:
            logical_mapping_str = f"uc_{env}_snt_fdn_01.{underlying_schema}"

        return logical_mapping_str
    else:
        raise ValueError(f"No mapping found for {logical_key} ...")

# COMMAND ----------

def extract_input_details(input_tbls):
    _db_name_to_metadata_key = {
        "bronze_masterdata": bronze_masterdata_db,
        "bronze_masterdata_confidential": bronze_masterdata_confidential_db,
        "bronze_masterdata_internal": bronze_masterdata_internal_db,
        "bronze_finance": bronze_finance_db,
        "bronze_finance_confidential": bronze_finance_confidential_db,
        "bronze_finance_confidential_raw": bronze_finance_confidential_raw_db,
        "bronze_finance_internal": bronze_finance_internal_db,
        "bronze_sales": bronze_sales_db,
        "bronze_sales_confidential": bronze_sales_confidential_db,
        "bronze_sales_internal": bronze_sales_internal_db,
        "bronze_procurement": bronze_procurement_db,
        "bronze_procurement_confidential": bronze_procurement_confidential_db,
        "bronze_procurement_internal": bronze_procurement_internal_db,
        "bronze_mosaic": mosaic_bronze_db,
        "masterdata": silver_masterdata_restricted_db,
        "masterdata_confidential": silver_masterdata_confidential_db,
        "masterdata_internal": silver_masterdata_internal_db,
        "masterdata_public": silver_masterdata_public_db,
        "finance": silver_finance_restricted_db,
        "finance_bvw": silver_finance_businessview_db,
        "finance_confidential": silver_finance_confidential_db,
        "finance_internal": silver_finance_internal_db,
        "finance_public": silver_finance_public_db,
        "manufacturing_restricted": silver_manufacturing_restricted_db,
        "manufacturing_confidential": silver_manufacturing_confidential_db,
        "manufacturing_internal": silver_manufacturing_internal_db,
        "manufacturing_public": silver_manufacturing_public_db,
        "gold": gold_db,
        "manual_gold": manual_gold_db,
        "spot3_gold":gold_db_spot3,
        "spot_gold":gold_db_spot,
        "ai_gold": ai_gold_db,
        "supplychain_confidential": silver_supplychain_confidential_db,
        "supplychain_internal": silver_supplychain_internal_db,
        "sales_public": sales_public_db,
        "sales_internal": sales_internal_db,
        "sales_restricted": sales_restricted_db,
        "sales_confidential": sales_confidential_db,
        "masterdata_p2d": silver_masterdata_restricted_p2d_db,
        "masterdata_confidential_p2d": silver_masterdata_confidential_p2d_db,
        "masterdata_internal_p2d": silver_masterdata_internal_p2d_db,
        "masterdata_public_p2d": silver_masterdata_public_p2d_db,
        "finance_p2d": silver_finance_restricted_p2d_db,
        "finance_view_p2d": silver_finance_view_p2d_db,
        "finance_bvw_p2d": silver_finance_businessview_p2d_db,
        "finance_confidential_p2d": silver_finance_confidential_p2d_db,
        "finance_internal_p2d": silver_finance_internal_p2d_db,
        "finance_public_p2d": silver_finance_public_p2d_db,
        "manufacturing_restricted_p2d": silver_manufacturing_restricted_p2d_db,
        "manufacturing_confidential_p2d": silver_manufacturing_confidential_p2d_db,
        "manufacturing_internal_p2d": silver_manufacturing_internal_p2d_db,
        "manufacturing_public_p2d": silver_manufacturing_public_p2d_db,
        "supplychain_confidential_p2d": silver_supplychain_confidential_p2d_db,
        "supplychain_internal_p2d": silver_supplychain_internal_p2d_db,
        "sales_public_p2d": sales_public_p2d_db,
        "sales_internal_p2d": sales_internal_p2d_db,
        "sales_restricted_p2d": sales_restricted_p2d_db,
        "sales_confidential_p2d": sales_confidential_p2d_db,
        "silver_derived": silver_derived_db,
        "bronze_masterdata_internal_p2d": bronze_masterdata_internal_p2d_db,
        "bronze_finance_p2d": bronze_finance_p2d_db,
        "bronze_finance_confidential_p2d": bronze_finance_confidential_p2d_db,
        "bronze_finance_internal_p2d": bronze_finance_internal_p2d_db,
        "bronze_sales_p2d": bronze_sales_p2d_db,
        "bronze_sales_confidential_p2d": bronze_sales_confidential_p2d_db,
        "bronze_sales_internal_p2d": bronze_sales_internal_p2d_db,
        "bronze_procurement_p2d": bronze_procurement_p2d_db,
        "bronze_procurement_confidential_p2d": bronze_procurement_confidential_p2d_db,
        "bronze_procurement_internal_p2d": bronze_procurement_internal_p2d_db,
        "masterdata_p2pp": silver_masterdata_restricted_p2pp_db,
        "masterdata_confidential_p2pp": silver_masterdata_confidential_p2pp_db,
        "masterdata_internal_p2pp": silver_masterdata_internal_p2pp_db,
        "masterdata_public_p2pp": silver_masterdata_public_p2pp_db,
        "finance_p2pp": silver_finance_restricted_p2pp_db,
        "finance_bvw_p2pp": silver_finance_businessview_p2pp_db,
        "finance_confidential_p2pp": silver_finance_confidential_p2pp_db,
        "finance_internal_p2pp": silver_finance_internal_p2pp_db,
        "finance_public_p2pp": silver_finance_public_p2pp_db,
        "manufacturing_restricted_p2pp": silver_manufacturing_restricted_p2pp_db,
        "manufacturing_confidential_p2pp": silver_manufacturing_confidential_p2pp_db,
        "manufacturing_internal_p2pp": silver_manufacturing_internal_p2pp_db,
        "manufacturing_public_p2pp": silver_manufacturing_public_p2pp_db,
        "supplychain_confidential_p2pp": silver_supplychain_confidential_p2pp_db,
        "supplychain_internal_p2pp": silver_supplychain_internal_p2pp_db,
        "sales_public_p2pp": sales_public_p2pp_db,
        "sales_internal_p2pp": sales_internal_p2pp_db,
        "sales_restricted_p2pp": sales_restricted_p2pp_db,
        "sales_confidential_p2pp": sales_confidential_p2pp_db,
        "bronze_masterdata_p2pp": bronze_masterdata_p2pp_db,
        "bronze_masterdata_confidential_p2pp": bronze_masterdata_confidential_p2pp_db,
        "bronze_masterdata_internal_p2pp": bronze_masterdata_internal_p2pp_db,
        "bronze_finance_p2pp": bronze_finance_p2pp_db,
        "bronze_finance_confidential_p2pp": bronze_finance_confidential_p2pp_db,
        "bronze_finance_internal_p2pp": bronze_finance_internal_p2pp_db,
        "bronze_sales_p2pp": bronze_sales_p2pp_db,
        "bronze_sales_confidential_p2pp": bronze_sales_confidential_p2pp_db,
        "bronze_sales_internal_p2pp": bronze_sales_internal_p2pp_db,
        "bronze_procurement_p2pp": bronze_procurement_p2pp_db,
        "bronze_procurement_confidential_p2pp": bronze_procurement_confidential_p2pp_db,
        "bronze_procurement_internal_p2pp": bronze_procurement_internal_p2pp_db
    }

    input_dets_missing_quotes = re.sub(":\s*(,|})",":\"\"\\1", input_tbls)
    swap_quotes = re.sub("'", '"', input_dets_missing_quotes)
    input_dets = json.loads(swap_quotes)

    res = []

    if unity_flag.upper() == "Y":
        for key in input_dets.keys():
            tables = [x.strip() for x in input_dets[key].split(',') if x not in ('', ' ')]

            for table in tables:
                logical_key = f"{key}.{table}"

                try:
                    db_name = get_logical_mapping(logical_key)
                except ValueError:
                    print(f"[INFO]: No UC mapping present for {table}, using original HMS mapping ...")
                    db_name = _db_name_to_metadata_key[key]

                if db_name:
                    res.append(f"{db_name}.{table}")
    else:
        for key in input_dets.keys():
            if key in _db_name_to_metadata_key:
                db_name = _db_name_to_metadata_key[key]
                res += list([db_name + "." + x.strip() for x in input_dets[key].split(',') if x not in ('', ' ')])

    return res


# This function is deprecated. Use extract_input_details instead.
def input_details_extract(input_tbls):
#   """1. Input parameters  
#     a. input_tbls - key-value pair. Eg. {"bronze_finance" : "tbl1, tbl2", "manual_gold" : "tbl3, tbl4"}
# 2. Purpose  
#     a. This function when provided with the input argument will return a list of (for the above input argument) [db1.tbl1, db1.tbl2, db2.tbl3, db2.tbl4]  
#     b. This is to accomodate dynamic chnages in the database names and the input sources  
#     c. Used with read_input function """
#   input_dets_missing_quotes = re.sub(":\s*(,|})",":\"\"\\1",input_tbls)
#   swap_quotes = re.sub("'",'"',input_dets_missing_quotes)
#   input_dets = json.loads(swap_quotes)
  
#   bronze_masterdata_ip, bronze_finance_ip, bronze_mosaic_ip, masterdata_restricted_ip, masterdata_confidential_ip, masterdata_internal_ip, masterdata_public_ip, finance_restricted_ip, finance_confidential_ip, finance_internal_ip, finance_public_ip, gold_ip, manual_gold_ip, supplychain_confidential_ip = [], [], [], [], [], [], [],[], [], [], [], [], [],[]
  
#   for key in input_dets.keys():
#     if key == 'bronze_masterdata':
#       bronze_masterdata_ip = list([bronze_masterdata_db + "." + x.strip() for x in list(input_dets[key].split(',')) if x not in ('', ' ')])
#     if key == 'bronze_finance':
#       bronze_finance_ip = list([bronze_finance_db + "." + x.strip() for x in list(input_dets[key].split(',')) if x not in ('', ' ')])
#     if key == 'bronze_mosaic':
#       bronze_mosaic_ip = list([mosaic_bronze_db + "." + x.strip() for x in list(input_dets[key].split(',')) if x not in ('', ' ')])
#     if key == 'masterdata':
#       masterdata_restricted_ip = list([silver_masterdata_restricted_db + "." + x.strip() for x in list(input_dets[key].split(',')) if x not in ('', ' ')])
#     if key == 'masterdata_confidential':
#       masterdata_confidential_ip = list([silver_masterdata_confidential_db + "." + x.strip() for x in list(input_dets[key].split(',')) if x not in ('', ' ')])
#     if key == 'masterdata_internal':
#       masterdata_internal_ip = list([silver_masterdata_internal_db + "." + x.strip() for x in list(input_dets[key].split(',')) if x not in ('', ' ')])
#     if key == 'masterdata_public':
#       masterdata_public_ip = list([silver_masterdata_public_db + "." + x.strip() for x in list(input_dets[key].split(',')) if x not in ('', ' ')])
#     if key == 'finance':
#       finance_restricted_ip = list([silver_finance_restricted_db + "." + x.strip() for x in list(input_dets[key].split(',')) if x not in ('', ' ')])
#     if key == 'finance_confidential':
#       finance_confidential_ip = list([silver_finance_confidential_db + "." + x.strip() for x in list(input_dets[key].split(',')) if x not in ('', ' ')])
#     if key == 'finance_internal':
#       finance_internal_ip = list([silver_finance_internal_db + "." + x.strip() for x in list(input_dets[key].split(',')) if x not in ('', ' ')])
#     if key == 'finance_public':
#       finance_public_ip = list([silver_finance_public_db + "." + x.strip() for x in list(input_dets[key].split(',')) if x not in ('', ' ')])
#     if key == 'gold':
#       gold_ip = list([gold_db + "." + x.strip() for x in list(input_dets[key].split(',')) if x not in ('', ' ')])
#     if key == 'manual_gold':
#       manual_gold_ip = list([manual_gold_db + "." + x.strip() for x in list(input_dets[key].split(',')) if x not in ('', ' ')])
#     if key == 'supplychain_confidential':
#       supplychain_confidential_ip = list([silver_supplychain_confidential_db + "." + x.strip() for x in list(input_dets[key].split(',')) if x not in ('', ' ')])

#   input_dets = bronze_masterdata_ip + bronze_finance_ip + bronze_mosaic_ip + masterdata_restricted_ip + masterdata_confidential_ip + masterdata_internal_ip + masterdata_public_ip + finance_restricted_ip + finance_confidential_ip + finance_internal_ip + finance_public_ip + gold_ip + manual_gold_ip + supplychain_confidential_ip
#   return input_dets
    _db_name_to_metadata_key = {
        "bronze_masterdata": bronze_masterdata_db,
        "bronze_masterdata_confidential": bronze_masterdata_confidential_db,
        "bronze_masterdata_internal": bronze_masterdata_internal_db,
        "bronze_finance": bronze_finance_db,
        "bronze_finance_confidential": bronze_finance_confidential_db,
        "bronze_finance_internal": bronze_finance_internal_db,
        "bronze_sales": bronze_sales_db,
        "bronze_sales_confidential": bronze_sales_confidential_db,
        "bronze_sales_internal": bronze_sales_internal_db,
        "bronze_procurement": bronze_procurement_db,
        "bronze_procurement_confidential": bronze_procurement_confidential_db,
        "bronze_procurement_internal": bronze_procurement_internal_db,
        "bronze_mosaic": mosaic_bronze_db,
        "masterdata": silver_masterdata_restricted_db,
        "masterdata_confidential": silver_masterdata_confidential_db,
        "masterdata_internal": silver_masterdata_internal_db,
        "masterdata_public": silver_masterdata_public_db,
        "finance": silver_finance_restricted_db,
        "finance_bvw": silver_finance_businessview_db,
        "finance_confidential": silver_finance_confidential_db,
        "finance_internal": silver_finance_internal_db,
        "finance_public": silver_finance_public_db,
        "manufacturing_restricted": silver_manufacturing_restricted_db,
        "manufacturing_confidential": silver_manufacturing_confidential_db,
        "manufacturing_internal": silver_manufacturing_internal_db,
        "manufacturing_public": silver_manufacturing_public_db,
        "gold": gold_db,
        "manual_gold": manual_gold_db,
        "spot3_gold":gold_db_spot3,
        "supplychain_confidential": silver_supplychain_confidential_db,
        "supplychain_internal": silver_supplychain_internal_db,
        "sales_public": sales_public_db,
        "sales_internal": sales_internal_db,
        "sales_restricted": sales_restricted_db,
        "sales_confidential": sales_confidential_db,
        "masterdata_p2d": silver_masterdata_restricted_p2d_db,
        "masterdata_confidential_p2d": silver_masterdata_confidential_p2d_db,
        "masterdata_internal_p2d": silver_masterdata_internal_p2d_db,
        "masterdata_public_p2d": silver_masterdata_public_p2d_db,
        "finance_p2d": silver_finance_restricted_p2d_db,
        "finance_bvw_p2d": silver_finance_businessview_p2d_db,
        "finance_confidential_p2d": silver_finance_confidential_p2d_db,
        "finance_internal_p2d": silver_finance_internal_p2d_db,
        "finance_public_p2d": silver_finance_public_p2d_db,
        "manufacturing_restricted_p2d": silver_manufacturing_restricted_p2d_db,
        "manufacturing_confidential_p2d": silver_manufacturing_confidential_p2d_db,
        "manufacturing_internal_p2d": silver_manufacturing_internal_p2d_db,
        "manufacturing_public_p2d": silver_manufacturing_public_p2d_db,
        "supplychain_confidential_p2d": silver_supplychain_confidential_p2d_db,
        "supplychain_internal_p2d": silver_supplychain_internal_p2d_db,
        "sales_public_p2d": sales_public_p2d_db,
        "sales_internal_p2d": sales_internal_p2d_db,
        "sales_restricted_p2d": sales_restricted_p2d_db,
        "sales_confidential_p2d": sales_confidential_p2d_db,
        "silver_derived": silver_derived_db,
        "bronze_masterdata_internal_p2d": bronze_masterdata_internal_p2d_db,
        "bronze_finance_p2d": bronze_finance_p2d_db,
        "bronze_finance_confidential_p2d": bronze_finance_confidential_p2d_db,
        "bronze_finance_internal_p2d": bronze_finance_internal_p2d_db,
        "bronze_sales_p2d": bronze_sales_p2d_db,
        "bronze_sales_confidential_p2d": bronze_sales_confidential_p2d_db,
        "bronze_sales_internal_p2d": bronze_sales_internal_p2d_db,
        "bronze_procurement_p2d": bronze_procurement_p2d_db,
        "bronze_procurement_confidential_p2d": bronze_procurement_confidential_p2d_db,
        "bronze_procurement_internal_p2d": bronze_procurement_internal_p2d_db,
        "masterdata_p2pp": silver_masterdata_restricted_p2pp_db,
        "masterdata_confidential_p2pp": silver_masterdata_confidential_p2pp_db,
        "masterdata_internal_p2pp": silver_masterdata_internal_p2pp_db,
        "masterdata_public_p2pp": silver_masterdata_public_p2pp_db,
        "finance_p2pp": silver_finance_restricted_p2pp_db,
        "finance_bvw_p2pp": silver_finance_businessview_p2pp_db,
        "finance_confidential_p2pp": silver_finance_confidential_p2pp_db,
        "finance_internal_p2pp": silver_finance_internal_p2pp_db,
        "finance_public_p2pp": silver_finance_public_p2pp_db,
        "manufacturing_restricted_p2pp": silver_manufacturing_restricted_p2pp_db,
        "manufacturing_confidential_p2pp": silver_manufacturing_confidential_p2pp_db,
        "manufacturing_internal_p2pp": silver_manufacturing_internal_p2pp_db,
        "manufacturing_public_p2pp": silver_manufacturing_public_p2pp_db,
        "supplychain_confidential_p2pp": silver_supplychain_confidential_p2pp_db,
        "sales_public_p2pp": sales_public_p2pp_db,
        "sales_internal_p2pp": sales_internal_p2pp_db,
        "sales_restricted_p2pp": sales_restricted_p2pp_db,
        "sales_confidential_p2pp": sales_confidential_p2pp_db,
        "bronze_masterdata_p2pp": bronze_masterdata_p2pp_db,
        "bronze_masterdata_confidential_p2pp": bronze_masterdata_confidential_p2pp_db,
        "bronze_masterdata_internal_p2pp": bronze_masterdata_internal_p2pp_db,
        "bronze_finance_p2pp": bronze_finance_p2pp_db,
        "bronze_finance_confidential_p2pp": bronze_finance_confidential_p2pp_db,
        "bronze_finance_internal_p2pp": bronze_finance_internal_p2pp_db,
        "bronze_sales_p2pp": bronze_sales_p2pp_db,
        "bronze_sales_confidential_p2pp": bronze_sales_confidential_p2pp_db,
        "bronze_sales_internal_p2pp": bronze_sales_internal_p2pp_db,
        "bronze_procurement_p2pp": bronze_procurement_p2pp_db,
        "bronze_procurement_confidential_p2pp": bronze_procurement_confidential_p2pp_db,
        "bronze_procurement_internal_p2pp": bronze_procurement_internal_p2pp_db
    }

    input_dets_missing_quotes = re.sub(":\s*(,|})",":\"\"\\1", input_tbls)
    swap_quotes = re.sub("'", '"', input_dets_missing_quotes)
    input_dets = json.loads(swap_quotes)

    res = []

    if unity_flag.upper() == "Y":
        for key in input_dets.keys():
            tables = [x.strip() for x in input_dets[key].split(',') if x not in ('', ' ')]

            for table in tables:
                logical_key = f"{key}.{table}"

                try:
                    db_name = get_logical_mapping(logical_key)
                except ValueError:
                    print(f"[INFO]: No UC mapping present for {table}, using original HMS mapping ...")
                    db_name = _db_name_to_metadata_key[key]

                if db_name:
                    res.append(f"{db_name}.{table}")
    else:
        for key in input_dets.keys():
            if key in _db_name_to_metadata_key:
                db_name = _db_name_to_metadata_key[key]
                res += list([db_name + "." + x.strip() for x in input_dets[key].split(',') if x not in ('', ' ')])

    return res

# COMMAND ----------

# MAGIC %md ####Reading from the input tables

# COMMAND ----------


# def read_input(input_dets, audit_config):
#   """1. Input parameters  
#     a. input_dets - comma-separated list of [db.tb] from which data is to be loaded.   
#     b. audit_config - used for Loganalytics - defined in each workflow
# 2. Purpose  
#     a. Loads data from tables into a dataframe dictionary
# 3. Scd2 check"""
  
#   STlen = len(input_dets)
#   auditLog('S', 'Starting load of data from the input and output tables', audit_config)

#   try:
#     df = {}
#     tbl_count = 0
#     scd_dict = {x.TableName:x.SCDLoadFlag for x in spark.sql('''SELECT TableName,SCDLoadFlag FROM mosaic_audit.scdloadconfig''').collect()}
#     for db_tbl_name in input_dets:
#         tbl_count = tbl_count + 1
#         db_name = db_tbl_name.split('.')[0]
#         tbl_name = db_tbl_name.split('.')[1]
#         if (db_tbl_name in scd_dict.keys()):
#             if (scd_dict[db_tbl_name] in ('Y','N')):
#                 df[tbl_name] = spark.sql(f"select * from {db_tbl_name} where ActiveFlag != 'N'")
#             #else:
#                 #df[tbl_name] = spark.sql(f"select * from {db_tbl_name}")
#         else:
#             df[tbl_name] = spark.sql(f"select * from {db_tbl_name}")
#         auditLog('S', f'Successfully loaded data from the input table ({tbl_count}/{STlen}): {db_tbl_name}', audit_config)
      
#     return df
#   except Exception as e:
#     auditLog('F', f'Loading of input tables failed due to exception: {e}', audit_config)
#     raise e


# COMMAND ----------

def read_input(input_dets, audit_config):
  """1. Input parameters  
    a. input_dets - comma-separated list of [db.tb] from which data is to be loaded.   
    b. audit_config - used for Loganalytics - defined in each workflow
2. Purpose  
    a. Loads data from tables into a dataframe dictionary
3. Scd2 check"""
  
  STlen = len(input_dets)
  #auditLog('S', 'Starting load of data from the input and output tables', audit_config)

  try:
    df = {}
    tbl_count = 0
    scd_dict = {x.TableName:x.SCDLoadFlag for x in spark.sql('''SELECT TableName,SCDLoadFlag FROM mosaic_audit.scdloadconfig''').collect()}
    scd_lower = dict((k.lower(), v) for k, v in scd_dict.items())
    for db_tbl_name in input_dets:
        tbl_count = tbl_count + 1
        db_name = db_tbl_name.rsplit('.', 1)[0]
        tbl_name = db_tbl_name.rsplit('.', 1)[1]
        db_tbl_name_lower = db_tbl_name.lower()
        if (db_tbl_name_lower in scd_lower.keys()):
            if (scd_lower[db_tbl_name_lower] in ('Y','N')):
                df[tbl_name] = spark.sql(f"select * from {db_tbl_name} where ActiveFlag != 'N'")
            #else:
                #df[tbl_name] = spark.sql(f"select * from {db_tbl_name}")
        else:
            df[tbl_name] = spark.sql(f"select * from {db_tbl_name}")
        #auditLog('S', f'Successfully loaded data from the input table ({tbl_count}/{STlen}): {db_tbl_name}', audit_config)
      
    return df
  except Exception as e:
    #auditLog('F', f'Loading of input tables failed due to exception: {e}', audit_config)
    raise e

# COMMAND ----------

# MAGIC %md ####Reading from the output tables - used mainly in DQ for comparing the previous load

# COMMAND ----------

def read_output(gold_db, gold_tblname, audit_config):
  """1. Input parameters  
    a. gold_db - database in which the tables exist. Currently, only supports one database for all the tables  
    b. gold_tblname - list of comma-separated table names
    b. audit_config - used for Loganalytics - defined in each workflow
2. Purpose  
    a. Loads data from tables into a dataframe dictionary
    b. Used for DQ rules primarily"""
  
  try:
    df_gold = {}
    i = 0
    for nm in gold_tblname:
      df_gold[nm] = spark.sql(f"select * from {gold_db}.{nm}")
      auditLog('S', f'Successfully loaded data from the output table : {nm}', audit_config)
      i = i+1
      
    return df_gold
  except Exception as e:
    auditLog('F', f'Loading of output tables failed due to exception: {e}', audit_config)
    raise e

# COMMAND ----------

# MAGIC %md #### Column mapping between source (Alteryx) and target (gold) - for ease of transforming logic and QA

# COMMAND ----------

# DBTITLE 1,Reading tables - Map the target columns to source and back
def col_mapping(source, target, mapping_type, df):
  """1. Input parameters  
    a. source - list of source columns such as TM1 columns or business-defined columns - should be one -on-one mapping with the target column and vice-versa  
    b. target - list of target columns such as goldtable columns   
    c. mapping_type - 'Source To Target' or 'Target To Source'  
    d. df - dataframe on which this operation has to happen  
2. Purpose    
    a. Ease of converting columns to transform business logic  
    b. Can add additional time to the workflow  """
  
  try:
    if mapping_type == 'Source To Target':
      dictt = dict(zip(source, target))
    if mapping_type == 'Target To Source':
      dictt = dict(zip(target, source))
    for key in dictt:
      val = dictt[key]
      df = df.withColumnRenamed(key,val)
    return df
  except Exception as e:
    print(e)
    raise e 

# COMMAND ----------

# MAGIC %md #### Datatype mapping with optional column mapping

# COMMAND ----------

# DBTITLE 1,Writing tables - Source to target data type mapping and column name mapping
def col_type_mapping(gold_db, gold_nm, df_Final, gold_cols, source_cols = [], colmap_flag = False):
  """1. Input parameters  
    a. gold_db - the database to which the table is to be written  
    b. gold_nm - the table name
    c. df_Final - pyspark dataframe with the data   
    d. gold_cols - the columns in the gold table excluding the audit columns such as TM1instance, CreatedById etc. Columns expected to be in df_Final  
    e. source_cols - needed in the event col_mapping function is used  
    f. colmap_flag - indicates if col_mapping needs to be used. Two options - True or False    
2. Purpose    
    a. Converts the dataframe datatypes to the target table's datatypes  
    b. Has the option of turning on/orr the colmap_flag"""
  
  try:
    col_data = spark.sql(f"desc {gold_db}.{gold_nm}").toPandas()
    common_cols = list(set(col_data['col_name'].tolist()) & set(gold_cols))
    
    
    if colmap_flag:
      gold_df_final = col_mapping(source_cols, gold_cols, 'Source To Target', df_Final).select(common_cols)
    else:
      gold_df_final = df_Final.select(common_cols)
    
    #datatypes in dataframes
    dtyp_vals = [list(x) for x in gold_df_final.dtypes]
    dtyp_vals_dict = {x[0]:x[1] for x in dtyp_vals}

    #Mapping the source datatype to the target datatype
    for coln in gold_cols:
      dat_type = (col_data[col_data['col_name'] == coln]['data_type']).tolist()[0]
      if dat_type != dtyp_vals_dict[coln]:
        if dat_type == 'date':
          gold_df_final = gold_df_final.withColumn(coln, to_date(col(coln),"yyyy-MM-dd"))
        else:
          gold_df_final = gold_df_final.withColumn(coln, col(coln).cast(dat_type))
    
    return gold_df_final

  except Exception as e:
    print('Mapping of the columns from source to target datatype failed.')
    raise e


# COMMAND ----------

# MAGIC %md #### Adding audit columns to the gold tables

# COMMAND ----------

# DBTITLE 1,Writing tables - Market column, TM1 and audit columns
def mkt_tm_audit(gold_df_final, TM1_instance, market, job_id):
  """1. Input parameters  
    a. gold_df_final - dataframe to which the audit columns need to be added   
    b. TM1_instance   
    c. market  
    d. job_id - passed from the ADF      
2. Purpose    
    a. Adds the audit columns to the dataframe.   
    b. Columns not required to be dropped in the workflow"""
  
  try:
    gold_df_final = gold_df_final.withColumn("TM1SystemCode", lit(TM1_instance))\
                                   .withColumn("MarketUnitName", lit(market))\
                                   .withColumn("CreatedTime", current_timestamp())\
                                   .withColumn("CreatedById", lit(job_id))\
                                   .withColumn("UpdatedTime", current_timestamp())\
                                   .withColumn("UpdatedById", lit(job_id))
    return gold_df_final
  except Exception as e:
    print('Error while updating the market, TM1 instance and the audit columns')
    raise e

# COMMAND ----------

# MAGIC %md #### Writing to Gold table - 2 functions for partitioned and non-partitioned tables

# COMMAND ----------

# MAGIC %md #####Deprecated - 
# MAGIC
# MAGIC The below three functions will be removed soon.  
# MAGIC  a. write_df_to_table  
# MAGIC  b. load_data  
# MAGIC  c. load_data_new

# COMMAND ----------

def write_df_to_table(df,dbname,tblname,writeMode,writeFormat="delta"):
  try:
    df.write.mode(writeMode).format(writeFormat).saveAsTable("{0}.{1}".format(dbname,tblname))
  except Exception as error_msg:
    print("Error while writing dataframe data to table {0}".format(error_msg))
    nbpath = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    error_string = str(error_msg)
    spark.sql(f"""Insert into {audit_db}.softexceptionlog values ('{nbpath}','MosaicCommonUtilities','write_df_to_table',from_utc_timestamp(current_timestamp,'US/Eastern'),'{market}',"{error_string}");""")

# COMMAND ----------

def load_data(market, gold_df_final, gold_db, gold_nm):
  try:
    
    gold_df_final = gold_df_final.repartition('MarketUnitName')
    gold_df_final.write.format("delta").mode("overwrite").option("replaceWhere", f"MarketUnitName == '{market}'")\
              .save(f'{goldAdls_path}/gold/database/{gold_nm}') 
                               
    auditLog('S', f'Finished writing into the gold table {gold_nm}.', audit_config)
  except Exception as e:
    auditLog('F', f'Writing to the gold table {gold_nm} failed due to exception: {e}', audit_config)
    raise e

# COMMAND ----------

def load_data_new(gold_df_final, gold_db, gold_nm, replace_string = "", partition_cols = [], mode = "overwrite_full"):
  try:
    snapshot_db = "finance_mosaic_snapshot_gold"
    snapshotPath = goldAdls_path+'/snapshot/database/'+gold_nm
    if mode == "overwrite_partition":
      gold_df_final = gold_df_final.repartition(*[col(cols) for cols in partition_cols])
      gold_df_final.write.format("delta").mode("overwrite").option("replaceWhere", replace_string).saveAsTable(f'{gold_db}.{gold_nm}')
    if mode == "overwrite_full":
      gold_df_final.write.format("delta").mode("overwrite").saveAsTable(f'{gold_db}.{gold_nm}')
    
    auditLog('S', f'Finished writing into the gold table {gold_nm}.', audit_config)
  except Exception as e:
    auditLog('F', f'Writing to the gold table {gold_nm} failed due to exception: {e}', audit_config)
    raise e

# COMMAND ----------

# MAGIC %md #####Snapshot creation function 

# COMMAND ----------

# def snapshot_creation_old(gold_df, gold_db, gold_nm, dq_RuleRunID, partition_cols):
#   """1. Input parameters  
#     a. gold_df - the final dataframe written into the gold table  
#     b. gold_db - the database to which the gold table is written - not limited to finance_mosaic_gold  
#     c. gold_nm - name of the table in gold_db to which the data is being written  
#     d. dq_RuleRunID - the dq rule run id gennerated in the master notebook  
#     e. partition_cols - the list of columns by which gold_db.gold_nm is partitioned 
#     f. backupFlag - This function can also be utilized for take backups of the various config files being used
# 2. Purpose  
#     a. This function creates a table {gold_nm}Snapshot in the snapshot database  
#     b. The snapshot table is partitioned by partition_cols and SnapshotTimestamp (=dq_RuleRunID)  
#     c. The function will automatically overwrite and merge schema chnages made to the underlying table
#     d. Used along with load_data_with_history function"""
  
#   try:
    
#     #Sanity checks and ordering columns ad per the gold table
#     gold_nm = gold_nm.lower()
#     cols_order = spark.sql(f"select * from {gold_db}.{gold_nm} limit 1").columns
#     gold_df = gold_df.select(cols_order)

#     #DB creation
#     snapshot_dBPath = goldAdls_path + "/snapshot/database"
#     spark.sql(f"create database if not exists {gold_db_snapshot} LOCATION '{snapshot_dBPath}'")

#     #Partition and non-partition columns
#     tblSchema = gold_df.schema
#     snap_col = ['SnapshotTimestamp']
#     partition_columns = partition_cols + snap_col
#     colsDDl_nonPartition = (','.join([field.simpleString() for field in tblSchema if field.simpleString().split(':')[0] not in partition_columns])).replace(':', ' ')
#     colsDDl_Partition = (','.join([field.simpleString() for field in tblSchema if field.simpleString().split(':')[0] in partition_columns])).replace(':', ' ') + ', SnapshotTimestamp string' if len(partition_columns) > 1 else 'SnapshotTimestamp string'

#     #Create table
#     snapshot_TblPath = snapshot_dBPath + '/' + gold_nm + 'snapshot'
#     snapshot_Tbl = gold_db_snapshot + '.' + gold_nm + 'snapshot'
#     ddl = f"create table if not exists {snapshot_Tbl} ({colsDDl_nonPartition}) partitioned by ({colsDDl_Partition}) LOCATION '{snapshot_TblPath}'"
#     spark.sql(ddl)
    
# #     print(partition_columns)
# #     print(colsDDL_Partition)
# #     print(colsDDl_nonPartition)
#     print(ddl)
    
#     #Load data
#     gold_df.select(cols_order).withColumn("SnapshotTimestamp", lit(dq_RuleRunID)).repartition(*[col(cols) for cols in partition_columns]).write.format("delta").mode("append").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable(f'{snapshot_Tbl}')
    
#     auditLog('S', f'Finished writing into the snapshot table {snapshot_Tbl}.', audit_config)    
    
#   except Exception as e:
#     auditLog('F', f'Writing to the snapshot table {snapshot_Tbl} failed', audit_config)
#     raise e

# COMMAND ----------

def snapshot_creation(gold_df, gold_db, gold_nm, dq_RuleRunID, partition_cols, backupFlag = False):
  """1. Input parameters  
    a. gold_df - the final dataframe written into the gold table  
    b. gold_db - the database to which the gold table is written - not limited to finance_mosaic_gold  
    c. gold_nm - name of the table in gold_db to which the data is being written  
    d. dq_RuleRunID - the dq rule run id gennerated in the master notebook  
    e. partition_cols - the list of columns by which gold_db.gold_nm is partitioned 
    f. backupFlag - This function can also be utilized for take backups of the various config files being used
2. Purpose  
    a. This function creates a table {gold_nm}Snapshot in the snapshot database  
    b. The snapshot table is partitioned by partition_cols and SnapshotTimestamp (=dq_RuleRunID)  
    c. The function will automatically overwrite and merge schema chnages made to the underlying table
    d. Used along with load_data_with_history function"""
  try:
    
    #Sanity checks and ordering columns ad per the gold table
    gold_nm = gold_nm.lower()
    gold_table = spark.sql(f"select * from {gold_db}.{gold_nm} limit 1")
    cols_order = [_col for _col in gold_table.columns if _col in gold_df.columns]
    gold_df = gold_df.select(cols_order)

    #DB creation
    db_path = "/backup/database" if backupFlag else "/snapshot/database"
    snapshot_dBPath = goldAdls_path + db_path
    
    
    db_path = "/backup/database" if backupFlag else "/snapshot/database"
    db_snapshot = backup_db if backupFlag else gold_db_snapshot
    snapshot_Tbl = db_snapshot + '.' + gold_nm + 'snapshot'
    spark.sql(f"create database if not exists {db_snapshot} LOCATION '{snapshot_dBPath}'")

    #Partition and non-partition columns
    tblSchema = gold_table.schema
    snap_col1 = ['SnapshotTimestamp']
    snap_col2 = ['MarketUnitName','SnapshotTimestamp']
    
    partition_columns = [x.lower() for x in partition_cols]
    
    colsDDl_nonPartition = (','.join([field.simpleString() for field in tblSchema])).replace(':', ' ') + ', SnapshotTimestamp string'
    colsDDl_Partition_list = (snap_col2 if "marketunitname" in partition_columns else snap_col1)
    colsDDl_Partition =','.join(colsDDl_Partition_list)
    snapshot_TblPath = snapshot_dBPath + '/' + gold_nm + 'snapshot'
    ddl = f"create table if not exists {snapshot_Tbl} ({colsDDl_nonPartition}) USING delta partitioned by ({colsDDl_Partition}) LOCATION '{snapshot_TblPath}'"
    print(f"The snapshot DDL is: {ddl}")
    spark.sql(ddl)
    
    # snapshot_TblPath = spark.sql(f"describe detail {snapshot_Tbl}").collect()[0]['location']
    gold_df.select(cols_order).withColumn("SnapshotTimestamp", lit(dq_RuleRunID)).repartition(*[col(cols) for cols in colsDDl_Partition_list]).write.format("delta").mode("append").option("overwriteSchema", "true").option("mergeSchema", "true").save(f'{snapshot_TblPath}')
    
    #auditLog('S', f'Finished writing into the snapshot table {snapshot_Tbl}.', audit_config)    
    
  except Exception as e:
    #auditLog('F', f'Writing to the snapshot table {snapshot_Tbl} failed', audit_config)
    raise e

# COMMAND ----------

# MAGIC %md #####Load data with history

# COMMAND ----------

def load_data_with_history(gold_df_final, gold_db, gold_nm, dq_RuleRunID, replace_string = "", partition_cols = [], mode = "overwrite_full", snapshot_flag = False):
  """
  1. Input parameters
    a. gold_df_final - the final dataframe written into the gold table
    b. gold_db - the database to which the gold table is written - not limited to finance_mosaic_gold
    c. gold_nm - name of the table in gold_db to which the data is being written
    d. dq_RuleRunID - the dq rule run id gennerated in the master notebook
    e. replace_string - replace string used for delta tables - delta tables do not support dynamic overwrite
    f. partition_cols - the list of columns by which gold_db.gold_nm is partitioned
    g. mode - overwrite_full - overwrites the entire table; overwrite_partition - partition-wise overwrite
    h. snapshot_flag - only takes True or False - if True will create a snapshot table

  2. Purpose
    a. This function created a table {gold_nm} in the {gold_db} database
    b. Table is partitioned by partition_cols
    c. Performs SCD Type-2 Data load if the table is configured for it, else performs Truncate/Load 
    d. Has the option to create a snapshot table
  """
  try:
    additional_params = dbutils.widgets.get("additional_params")
    addtnl_params_missing_quotes = re.sub(":\s*(,|})", ":\"\"\\1", additional_params)
    change_quotes = re.sub("'", '"', addtnl_params_missing_quotes)
    addtnl_params = json.loads(change_quotes)
    SynapseFlag = addtnl_params.get("SynapseFlag","S")
  except Exception as e:
    dbutils.widgets.text('SynapseFlag','')
    SynapseFlag = dbutils.widgets.get("SynapseFlag")
    if SynapseFlag in ('S','D','SD'):
      print('Getting SynapseFlag from ADF, as unable to parse additional params')
    else:
      print('Synapse flag is missing in additional params, hence defaulting to S')
      print(e)
      SynapseFlag = "S"

  print(f"synapse flag : {SynapseFlag}") 
  if unity_flag.upper() == "Y":
    Gold_Table = gold_db.split(".")[-1] + "." + gold_nm
  else:
    Gold_Table = gold_db + "." + gold_nm
        
  print(Gold_Table)
  MarketName = dbutils.widgets.get("market")

  if SynapseFlag == "D" or SynapseFlag == "SD": 
    try:
      TM1LoadWaitDBSQL(Gold_Table, MarketName)
      DBSQLRunStatusUpdate(Gold_Table, MarketName, 'DBSQLRunStart')
    except Exception as error:
      DBSQLRunStatusUpdate(Gold_Table, MarketName, 'DBSQLRunError')
      print("Error occured while updating data transamission control wwhile writing to gold Table {0}".format(error))
      raise error

  try:
    MarketName = dbutils.widgets.get("market")    
    input_tbls = dbutils.widgets.get("input_tbls")
    synapse_tbls = dbutils.widgets.get("synapse_tbls")
    syn_schema_tbls = [synapse_db + "." + x.strip() for x in list(synapse_tbls.split(',')) if x not in ('', ' ')]
    TargetTableName = gold_db + "." + gold_nm
    ConfigDF = spark.sql(f"SELECT IFNULL((SELECT SCDLoadFlag FROM mosaic_audit.scdloadconfig WHERE TableName = '{TargetTableName}'), 'N') AS SCDLoadFlag")
    SCDLoadFlag = ConfigDF.collect()[0].SCDLoadFlag

    if SCDLoadFlag == "Y":
      SCDType2DataLoad(MarketName, gold_df_final, TargetTableName)

    else:
      tblPath = spark.sql(f"describe detail {gold_db}.{gold_nm}").collect()[0]['location']
      if mode == "overwrite_partition":
        gold_df_final = gold_df_final.repartition(*[col(cols) for cols in partition_cols])
        gold_df_final.write.format("delta").mode("overwrite").option("replaceWhere", replace_string).save(f'{tblPath}')
      if mode == "overwrite_full":
        gold_df_final.write.format("delta").mode("overwrite").save(f'{tblPath}')
      if mode == "append":
        gold_df_final.write.format("delta").mode("append").save(f'{tblPath}')

    if snapshot_flag:
      snapshot_creation(gold_df_final, gold_db, gold_nm, dq_RuleRunID, partition_cols)

    # Retrieving Record Counts
    try:
      InsertedRecordCount, UpdatedRecordCount, DeletedRecordCount = GetDeltaTableLoadStats(TargetTableName)
    except Exception as error:
      InsertedRecordCount, UpdatedRecordCount, DeletedRecordCount = 0, 0, 0
    
    # Retrieving Source/Target/Synapse table details
    input_tbls_arr = input_details_extract(input_tbls) if input_tbls != '' else []
    synapse_tbls = syn_schema_tbls[0] if len(syn_schema_tbls) != 0 else ''
    InputTables = ''
    for TableName in input_tbls_arr:
      InputTables = InputTables + ',' + TableName
    tableDetails = { "market": f"{MarketName}", "input_tbls": f"{InputTables[1:]}", "target_tbl": f"{TargetTableName}", "synapse_tbl": f"{synapse_tbls}" }
      
    recordCounts = {"inserted_ct": f"{InsertedRecordCount}", "updated_ct": f"{UpdatedRecordCount}", "deleted_ct": f"{DeletedRecordCount}"}
    additional_context = {**tableDetails, **recordCounts}
    audit_config["additional_context"] = additional_context
    auditLog('S', f'[INFO]: Completed Loading of Gold Layer Table (and history table) - {gold_nm}.', audit_config)

    if SynapseFlag == "D" or SynapseFlag == "SD":
      DBSQLRunStatusUpdate(Gold_Table, MarketName, 'DBSQLRunSuccess')
      
  except Exception as e:
    auditLog('F', f'Writing to the gold table (and history table) {gold_nm} failed', audit_config)
    if SynapseFlag == "D" or SynapseFlag == "SD":
      DBSQLRunStatusUpdate(Gold_Table, MarketName, 'DBSQLRunError')
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ### TPM/FTS Email Functions

# COMMAND ----------

# DBTITLE 1,sendEmailNotificationgeneric_TPM_FTS
def sendEmailNotificationgeneric_TPM_FTS(content, receiver, subject, message, attachmentname):
    from datetime import datetime
    import pytz
    try:
        env_details_dict ={'cdomosaicdevblob':'DEV','cdomosaicqablob':'QA','cdomosaicpreprodblob':'PRE-PROD','cdomosaicprodblob':'PROD'}
        env_details = env_details_dict[MosaicBlob]
        truncated_msg = ''
        now = datetime.now().replace(microsecond=0)
        now_cst = datetime.now(pytz.timezone('US/Central')).replace(microsecond=0).replace(tzinfo=None)
        now_est = datetime.now(pytz.timezone('US/Eastern')).replace(microsecond=0).replace(tzinfo=None)
        attachmentContent = content
        emailJSON = {'receiver': receiver,
                 'subject':subject,
                 'message':f'{message} <b>{env_details}</b> env executed at <br> {now} <b>UTC</b> <br> {now_cst} <b>CST</b> <br> {now_est} <b>EST</b>.',
                 'EmailType': 'mosaic_dq',
                 'attachmentName':attachmentname+'.csv',
                 'attachmentContent':attachmentContent}
        logic_app_post_url = dbutils.secrets.get(scope=adbScope ,key="LogicAppURL")
        response = requests.post(logic_app_post_url, json=emailJSON)
        return response
    except Exception as e:
        print(e)
        return 'failed to send email' 

# COMMAND ----------

# DBTITLE 1,sendEmailReport
def sendEmailReport(df_reconciled, dataset):
    """
    df_reconciled: dataframe containing data
    dataset: Name of dataset reconciled (e.g., 'TPM', 'FTS')
    """

    print(dataset)
    # check counts for all dates until current day
    #no_difference = (df_reconciled.filter((col("POSTINGDATE") < currentday)).agg(sum(abs("EHANA_minus_SILVER"))).collect()[0][0] == 0)
    no_difference = True
    if addtnl_params_dets.get('sendEmail') == "Y":
        #CSV file Generation
        op = prepareCsvAttachmentgeneric(df_reconciled)

        subject = f'{dataset} {market} Validation Report'
        message_prefix = f'Please find the {dataset} {market} validation report in '
        message = message_prefix # You can add more to the message later if needed

        # if no_difference:
        #     message = f"No difference found between eHana and silver datalake, Counts are matching\n {message}"
        # else:
        #     message = f"There are differences found between eHana and silver datalake, please verify the differences and take appropriate action\n {message}"
        attachmentname = f'{dataset} {market} Report'
        email = addtnl_params_dets.get('EmailId')

        sendEmailNotificationgeneric_TPM_FTS(op, email, subject, message, attachmentname)

# COMMAND ----------

# DBTITLE 1,format_no_scientific_pyspark
def format_no_scientific_pyspark(df, decimals=2):
    """
    Converts all float/double columns in a PySpark DataFrame to formatted strings.
    No scientific notation will appear after export or in output.
    """
    float_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() in ['double', 'float']]
    exprs = [
        format_string(f"%.{decimals}f", col(c)).alias(c) if c in float_cols else col(c)
        for c in df.columns
    ]
    return df.select(*exprs)

# COMMAND ----------

# MAGIC %md ####Writing to Synapse Table

# COMMAND ----------

loaded_tbls_count = 0

# COMMAND ----------

if unity_flag.upper() == "Y":
  import pyodbc

def writeToSynapse(df,writeMode,tableName,preActionsSQL):
  try:
    additional_params = dbutils.widgets.get("additional_params")
    addtnl_params_missing_quotes = re.sub(":\s*(,|})", ":\"\"\\1", additional_params)
    change_quotes = re.sub("'", '"', addtnl_params_missing_quotes)
    addtnl_params = json.loads(change_quotes)
    SynapseFlag = addtnl_params.get("SynapseFlag","S")
  except Exception as e:
    dbutils.widgets.text('SynapseFlag','')
    SynapseFlag = dbutils.widgets.get("SynapseFlag")
    if SynapseFlag in ('S','D','SD'):
      print('Getting SynapseFlag from ADF, as unable to parse additional params')
    else:
      print('Synapse flag is missing in additional params, hence defaulting to S')
      print(e)
      SynapseFlag = "S"

  print(f"synapse flag : {SynapseFlag}")  
  if SynapseFlag == "S" or SynapseFlag == "SD": 
    try:  
      tbl_nm = tableName.split(".")[1]
      MarketName = dbutils.widgets.get("market")
      
      TM1LoadWait(tbl_nm, MarketName)
      SynapseLoadStatusUpdate(tbl_nm, MarketName, 'SynapseLoadStart')
        
          
      df.write \
      .mode(writeMode) \
      .format("com.databricks.spark.sqldw") \
      .option("url", sqlDwUrl) \
      .option("tempDir", tempDir) \
      .option("enableServicePrincipalAuth","true") \
      .option("useAzureMSI", "true")\
      .option("preActions",preActionsSQL)\
      .option("dbTable", tableName) \
      .save()
      
      SynapseLoadStatusUpdate(tbl_nm, MarketName, 'SynapseLoadSuccess')
       
    except Exception as error:
      SynapseLoadStatusUpdate(tbl_nm, MarketName, 'SynapseLoadError')
      print("Error occured while writing to Synapse Table {0}".format(error))
      raise error
  else:
    print("Skipping writeToSynapse")

# COMMAND ----------

# MAGIC %md ### Orchestration functions

# COMMAND ----------

# MAGIC %md #### SynapseConnection

# COMMAND ----------

def SynapseConnection():
  import msal
  # Generate an OAuth2 access token for service principal, create spark properties and pass access token
  authority = f"https://login.windows.net/{tenantID}"
  token = msal.ConfidentialClientApplication(appID, dbutils.secrets.get(scope=adbScope,key=DESpnSecret), authority)\
                  .acquire_token_for_client(scopes="https://database.windows.net/.default")["access_token"]
  properties = spark._sc._gateway.jvm.java.util.Properties()
  properties.setProperty("accessToken", token)
  
  # Fetch the driver manager from your spark context and create a connection object
  driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager
  conn = driver_manager.getConnection(sqlDwUrl, properties)
  return conn

# COMMAND ----------

def buildSynapseToken():
    import msal
    import struct
    # Generate an OAuth2 access token for service principal, create spark properties and pass access token
    authority = f"https://login.windows.net/{tenantID}"
    token = msal.ConfidentialClientApplication(appID, dbutils.secrets.get(scope=adbScope,key=DESpnSecret), authority)\
                  .acquire_token_for_client(scopes="https://database.windows.net/.default")["access_token"]

    tokenb = bytes(token, "UTF-8")
    exptoken = b'';
    
    for i in tokenb:
        exptoken += bytes({i});
        exptoken += bytes(1);
        tokenstruct = struct.pack("=i", len(exptoken)) + exptoken;
    #print(tokenstruct)

    return tokenstruct

# COMMAND ----------

# MAGIC %md #### IndividualTM1CurrentStatusCheck

# COMMAND ----------

def IndividualTM1CurrentStatusCheck(TblName, MarketName):
  """ This function is used to check if any of the tables correspoding to a workflow are currently being loaded by TM1 or a previous load has failed"""
  if unity_flag.upper() == "Y":
    tokenstruct = buildSynapseToken()

    conn_str = (
      "DRIVER={ODBC Driver 18 for SQL Server};"
      f'SERVER=tcp:{dwServer},1433;'
      f'DATABASE={dwDatabase};'
      "Encrypt=yes;"
      "TrustServerCertificate=no;"
    )
  
    SQL_COPT_SS_ACCESS_TOKEN = 1256
    with pyodbc.connect(conn_str, attrs_before = {SQL_COPT_SS_ACCESS_TOKEN:tokenstruct}) as conn:

      with conn.cursor() as cursor:
        statement = f"""
        DECLARE @ActiveNErrorTablesCount INT;
        EXEC {synapse_config_db}.IndividualTM1CurrentStatusCheck ?, ?, @ActiveNErrorTablesCount OUTPUT;
        SELECT @ActiveNErrorTablesCount AS ActiveNErrorTablesCount;
        """

        cursor.execute(statement, TblName, MarketName)

        row = cursor.fetchone()
        if row:
          TblsCount = row.ActiveNErrorTablesCount
        else: 
          print("Nothing returned from procedure ...")
  else:
    statement = f"""EXEC {synapse_config_db}.IndividualTM1CurrentStatusCheck '{TblName}', '{MarketName}', ?"""
    con = SynapseConnection()
    exec_statement = con.prepareCall(statement)

    # Register the output parameters with their index and datatype (indexing starts at 1), Execute the statement and fetch the result with the correct method (get<javaType>)
    exec_statement.registerOutParameter(1, spark._sc._gateway.jvm.java.sql.Types.INTEGER)
    exec_statement.execute()
    TblsCount = exec_statement.getInt(1)

    # Close connections
    exec_statement.close()
    con.close()

  return TblsCount

# COMMAND ----------

# MAGIC %md #### SynapseLoadStatusUpdate

# COMMAND ----------

def SynapseLoadStatusUpdate(TblName, MarketName, LoadStatus):
  """This function updates the Synapse data load status in the control table"""
  statement = f"""EXEC {synapse_config_db}.SynapseLoadStatusUpdate '{TblName}', '{MarketName}', '{LoadStatus}'"""
  if unity_flag.upper() == "Y":
    tokenstruct = buildSynapseToken()

    conn_str = (
      "DRIVER={ODBC Driver 18 for SQL Server};"
      f'SERVER=tcp:{dwServer},1433;'
      f'DATABASE={dwDatabase};'
      "Encrypt=yes;"
      "TrustServerCertificate=no;"
    )

    SQL_COPT_SS_ACCESS_TOKEN = 1256 
    with pyodbc.connect(conn_str, attrs_before = {SQL_COPT_SS_ACCESS_TOKEN:tokenstruct}) as conn:

      with conn.cursor() as cursor:
          cursor.execute(statement)
  else:
    con = SynapseConnection()
    exec_statement = con.prepareCall(statement)
    exec_statement.execute()

    # Close connections
    exec_statement.close()
    con.close()

# COMMAND ----------

# MAGIC %md #### TM1LoadWait

# COMMAND ----------

def TM1LoadWait(TblName, MarketName):
  """This function implements waiting till all the tables have been marked as success for a specific workflow before beginning data load"""
  ActiveNErrorTablesCount = IndividualTM1CurrentStatusCheck(TblName, MarketName)
  while ActiveNErrorTablesCount != 0:
    print(f"TM1 table load is in progress. Waiting for 60 seconds before retrying data load...")
    time.sleep(60) 
    ActiveNErrorTablesCount = IndividualTM1CurrentStatusCheck(TblName, MarketName)
    
    if ActiveNErrorTablesCount == 0:   #fail-proof exit
      ActiveNErrorTablesCount = 0

# COMMAND ----------

# MAGIC %md ### Special functions

# COMMAND ----------

# MAGIC %md #### SalesGeo

# COMMAND ----------

def SalesGeoFunc(df_in, silver_master_db, ACDOCAFlag = False, PRPS_flag = False, CostCenterCodeColName ="CostCenterCode", ProfitCenterCodeColName ="ProfitCenterCode", ZsystemId = "H1N", ClientId = "200"):
  """Used for extracting the SalesGeo column as this is present in several workflows across Topline and Opex"""
  
  substring_CostCenterCDV = f"substring({CostCenterCodeColName}, 1, 4)"
  df = df_in.withColumn("SiteCode", expr(substring_CostCenterCDV))
  
  if ACDOCAFlag:
    df = df.withColumn("SiteCode", when(col("SiteCode").isNull(), col("PlantCode")).otherwise(col("SiteCode")))
  if PRPS_flag:
    df = df.withColumn("SiteCode", col("PlantCode"))
    
  dff_XTNObjectCharacteristicValueData = spark.sql(f"select * from {silver_master_db}.XTNObjectCharacteristicValueData where ZSystemId = '{ZsystemId}' and ClientID = '{ClientId}' and ClassType = '030'")\
                  .withColumn("CharacteristicValueCntr",col("CharacteristicValueCounter").cast("int"))\
                  .drop("XTNDFSystemId", "XTNDFReportingUnitId", "XTNCreatedTime", "XTNCreatedById", "XTNUpdatedTime", "XTNUpdatedById", "MarketUnitName")\
                  .filter(col("ZDeleteInd").isNull())

  dff_XTNObjectCharacteristicValueData.groupBy("ClientId","ClassifiedObjectKey",\
                "InternalCharacteristic","ObjectClassInd","ClassType","ECMArchivingObjectsInternalCounter")\
                .max("CharacteristicValueCntr")
    
  dff_XTNObjectCharacteristicValueData = dff_XTNObjectCharacteristicValueData.drop("CharacteristicValueCntr")
  
  dff_XTNObjectCharacteristicValueData = dff_XTNObjectCharacteristicValueData\
                  .withColumn("ClassifiedObjectKey_last4", expr("substring(ClassifiedObjectKey, -4, 4)"))
  
  df_GEO = df.alias("a").join(dff_XTNObjectCharacteristicValueData.alias("b"),\
                              on = df.SiteCode == dff_XTNObjectCharacteristicValueData.ClassifiedObjectKey_last4,
                              how = 'left')\
                 .select("a.*", "b.CharacteristicValue")\
                 .withColumn("SalesGeo", concat(col("CharacteristicValue"), lit("_"), col(ProfitCenterCodeColName)))\
                 .drop("CharacteristicValue")\
                 .dropDuplicates()
  return df_GEO

# COMMAND ----------

# MAGIC %md #### Deduplicating hierarchy levels in Product, Customer, LocalOrganization and goToMarket (Active records before running rule engine)

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

def hierarchyDeDupActiveRecordsMaxVal(levelcode, levelname, maxcolname, inputdf):

  """levelcode is the name of the level code column as string.
  levelname is the name of the level name column as string.
  maxcolname is the name of the column as string on which we do the maximize aggregate operation.
  inputdf is the dataframe containing the problematic data.
  """
  
  inputdf_nonnull = inputdf\
      .filter(col(levelcode).isNotNull())\
      .filter(col(levelname).isNotNull())\
      .filter(col(maxcolname).isNotNull())\
      .filter(col(levelcode) != "")\
      .filter(col(levelname) != "")\
      .filter(col(maxcolname) != "")

  inputtemptbl = "levelHierarchy"
  inputdf_nonnull.createOrReplaceTempView(inputtemptbl)

  levelnamenondup = levelname + "NonDup"
  df_local_hierarchy_max = spark.sql(f"""select {levelcode}, {levelname}, max(cast({maxcolname} as int)) as MaxVal
      from (select distinct {levelcode}, {levelname}, {maxcolname} 
      from {inputtemptbl}) 
      group by {levelcode}, {levelname}""")
  Windowquery =  Window.partitionBy(levelcode).orderBy(col("MaxVal").desc())
  
  df_local_hierarchy_max_rank1 = df_local_hierarchy_max\
      .withColumn("Rank",row_number().over(Windowquery))\
      .filter(col("Rank")==1)\
      .drop("Rank", "MaxVal")\
      .dropDuplicates()\
      .withColumnRenamed(levelname, levelnamenondup)

  df_local_hierarchy_nondup = inputdf.join(df_local_hierarchy_max_rank1, levelcode,"left")\
  .withColumn(levelname,when(col(levelnamenondup).isNull(),col(levelname))\
      .otherwise(col(levelnamenondup))).drop(levelnamenondup)
  
  return df_local_hierarchy_nondup

# COMMAND ----------

# MAGIC %md #### Deduplicating hierarchy levels in Product, Customer, LocalOrganization and goToMarket (Deleted records present in gold table)

# COMMAND ----------

def hierarchyDeDupDeletedRecordsMaxVal(levelcode, levelname, maxcolname, inputdf):

  """levelcode is the name of the level code column as string.
  levelname is the name of the level name column as string.
  maxcolname is the name of the column as string on which we do the maximize aggregate operation.
  inputdf is the dataframe containing the gold table data for a specific market.
  """

  inputtemptbl = "goldTbl"
  inputdf.createOrReplaceTempView(inputtemptbl)

  inputdf_D = inputdf.filter(col("ActiveFlag")=='D')
  inputdf_nonD = inputdf.filter(col("ActiveFlag")!='D')

  levelnamenondup_active = levelname + "NonDupActive"
  levelnamenondup_deleted = levelname + "NonDupDeleted"
  levelname_active = levelname + "Active"


  df_local_hierarchy_max_Active = spark.sql(f"""select {levelcode}, {levelname}, max(cast({maxcolname} as int)) as MaxVal
      from (select distinct {levelcode}, {levelname}, {maxcolname} 
      from {inputtemptbl} where ActiveFlag = 'Y') 
      group by {levelcode}, {levelname}""")
  df_local_hierarchy_max_Deleted = spark.sql(f"""select {levelcode}, {levelname}, max(cast({maxcolname} as int)) as MaxVal
      from (select distinct {levelcode}, {levelname}, {maxcolname} 
      from {inputtemptbl} where ActiveFlag = 'D') 
      group by {levelcode}, {levelname}""")
  df_local_hierarchy_Active = spark.sql(f"""select {levelcode}, {levelname} as {levelname_active}
      from (select distinct {levelcode}, {levelname} 
      from {inputtemptbl} where ActiveFlag = 'Y')""")
  Windowquery =  Window.partitionBy(levelcode).orderBy(col("MaxVal").desc())
  
  df_local_hierarchy_max_rank1_Active = df_local_hierarchy_max_Active\
      .withColumn("Rank",row_number().over(Windowquery))\
      .filter(col("Rank")==1)\
      .drop("Rank", "MaxVal")\
      .dropDuplicates()\
      .withColumnRenamed(levelname, levelnamenondup_active)
  df_local_hierarchy_max_rank1_Deleted = df_local_hierarchy_max_Deleted\
      .withColumn("Rank",row_number().over(Windowquery))\
      .filter(col("Rank")==1)\
      .drop("Rank", "MaxVal")\
      .dropDuplicates()\
      .withColumnRenamed(levelname, levelnamenondup_deleted)

  df_local_hierarchy_nondup = inputdf_D.join(df_local_hierarchy_max_rank1_Active, levelcode, "left")\
      .join(df_local_hierarchy_max_rank1_Deleted, levelcode, "left")\
      .join(df_local_hierarchy_Active, levelcode, "left")\
      .withColumn(levelname, when(col(levelnamenondup_active).isNull(),\
          when(col(levelnamenondup_deleted).isNull(),\
            when(col(levelname_active).isNull(),col(levelname))\
            .otherwise(col(levelname_active)))\
          .otherwise(col(levelnamenondup_deleted)))\
      .otherwise(col(levelnamenondup_active)))\
      .drop(levelnamenondup_active, levelnamenondup_deleted, levelname_active)

  df_local_hierarchy_nondup_union = inputdf_nonD.union(df_local_hierarchy_nondup.select(inputdf_nonD.columns))
  
  return df_local_hierarchy_nondup_union

# COMMAND ----------

# MAGIC %md ##DQ Framework functions

# COMMAND ----------

# MAGIC %md ##Function for Process Log

# COMMAND ----------

def RecordNotebookRunLogs(RecordType, MarketUnit, Category, ProcessName, RuleRunId, ProcessStartTime, Status, PeriodParameter = ''):
  """
    This Function makes an entry to Audit table when the Notebook execution starts and updates the entry once the execution completes.
    Input Parameters:
      RecordType - Indicates whether to Process Initiation or Closure Entry (Insert/Update)
      MarketUnit - Market Unit Name e.g., USA_PBNA
      Category - Module Name e.g., TOPLINE/OPEX/COGS
      ProcessName - ADF Pipeline Name for which this Notebook is executed e.g., CUSTOMER
      RuleRunId - Batch ID of Execution
      ProcessStartTime - Execution Start Time of Process (In String format: YYYY-MM-DD HH:mm:SS)
      Status - Status of Notebook execution (InProgress/Succeeded/Failed)
      PeriodParameter - Period Parameter used to execute the process e.g., 2022-001 (Optional Parameter)
    Output Parameters: None
  """

  from datetime import datetime
  current_datetime = datetime.now()                             # Current Time in UTC format
  CurrentTime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")  # Converting Timestamp to string format

  # Setting up Azure SQL Server Connection
  azureSQLDBServer = dbutils.secrets.get(scope=adbScope, key="mosaic-sql-server")
  SQLDatabase = dbutils.secrets.get(scope=adbScope, key="mosaic-sql-database")
  SQLUser = dbutils.secrets.get(scope=adbScope, key="mosaic-ui-sql-user")
  SQLPassword = dbutils.secrets.get(scope=adbScope, key="mosaic-ui-sql-password")

  jdbcUrl = "jdbc:sqlserver://" + azureSQLDBServer + ":1433;databaseName=" + SQLDatabase + ";user=" + SQLUser + ";password=" + SQLPassword + ";encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
  LogTable = "dbo.tbl_Mosaic_DBNotebookRunLogs"

  if RecordType == 'Insert':
    logSchema = ["MarketUnit", "Category", "ProcessName", "RuleRunId", "ProcessStartTime", "Status", "PeriodParameter", "RecordedTime"]
    logData = [(MarketUnit, Category, ProcessName, RuleRunId, ProcessStartTime, Status, PeriodParameter, ProcessStartTime)]
    logDataDF = spark.createDataFrame(data=logData, schema=logSchema)

  elif RecordType == 'Update':
    logSchema = ["MarketUnit", "Category", "ProcessName", "RuleRunId", "ProcessStartTime", "ProcessEndTime", "Status", "PeriodParameter", "RecordedTime"]
    logData = [(MarketUnit, Category, ProcessName, RuleRunId, ProcessStartTime, CurrentTime, Status, PeriodParameter, CurrentTime)]
    logDataDF = spark.createDataFrame(data=logData, schema=logSchema)

  else:
    dbutils.notebook.exit("[ERROR]: Selected Record Type is INVALID !!!")

  try:
    (logDataDF.write.format("jdbc")
     .mode("append")
     .option("url", jdbcUrl)
     .option("dbtable", LogTable)
     .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
     .save()
    )

    print("[INFO]: Entry into Log Table [", LogTable , "] Created Successfully !!!")

  except Exception as error:
    print("[ERROR]: Failed while Inserting log entry into Log Table [", LogTable , "] !!!")
    raise error

# COMMAND ----------

# MAGIC %md #### Function for performing SCD Type-2 Data Load

# COMMAND ----------

def SCDType2DataLoad(MarketUnit, SourceDataDF, TargetTableName):
  """
    This Function loads Target Table using Slowly Changing Dimension (SCD) Type-2 approach.
    It performs one of the below actions:
    1. NO ACTION: No changes received for the records in target table
    2. UPSERT: Attributes have changed in source. Existing records are inactivated and new records are inserted
    3. INSERT: New records received in source that are inserted into target table
    4. DELETE: Existing records not received in source and equivalent records in target table are inactivated

    Input Parameters:
    1. MarketUnit - Market Unit Name for which data is loaded
    2. SourceDataDF - DataFrame having Final Data set that needs to be loaded to Target Table
    3. TargetTableName - Fully Qualified Target Table e.g., finance_mosaic_gold.localorganization

    Output Parameters: None

    Notes:
    1. Create one entry for each table in Config Table "mosaic_audit.scdloadconfig"
    2. Config Entry: Target Table Name, SCDLoadFlag, KeyAttributes, NonTrackingAttributes
    3. Multiple columns to be separated by ";"
    4. Target Table should have attributes: EffectiveStartDate, EffectiveEndDate, ActiveFlag
    5. Target Table should be partioned on MarketUnitName attribute
  """

  StepNo = 0

  try:
    # Importing Libraries
    from pyspark.sql.functions import xxhash64, concat_ws, col, lit, when, current_date, to_date
    from delta.tables import DeltaTable

    # Reading Config Table & Deriving all required configurations
    ConfigDataDF = spark.sql(f"SELECT * FROM {audit_db}.scdloadconfig WHERE TableName = '{TargetTableName}'")
    ConfigData = ConfigDataDF.collect()[0]
    KeyAttributes = ConfigData.KeyAttributes.split(";")
    NonTrackingAttributes = [] if ConfigData.NonTrackingAttributes is None else ConfigData.NonTrackingAttributes.split(";")
    TgtTableAttributes = spark.table(TargetTableName).columns
    [TgtTableAttributes.remove(column) for column in KeyAttributes + NonTrackingAttributes]
    # TrackingAttributes = TgtTableAttributes
    TrackingAttributes = [_col for _col in TgtTableAttributes if _col in SourceDataDF.columns]  # Validate gold columns against source df.
    AllAttributes = KeyAttributes + TrackingAttributes + NonTrackingAttributes

    print(f"[INFO]: Starting Load for Table [{TargetTableName}] using Data Retention (SCD) Mechanism.")

    # Step 1: Creating DataFrame for Target Data Set
    targetDF = (spark.table(TargetTableName)
                .filter(f"MarketUnitName == '{MarketUnit}'")
                .filter("ActiveFlag == 'Y' or ActiveFlag == 'D'")
                .select(*AllAttributes
                        , xxhash64(*TrackingAttributes).alias("target_hash")
                        , concat_ws('|', *KeyAttributes).alias("target_MERGEKEY"))
               )

    RenameTgtAttributes = ["tgt_" + column for column in AllAttributes]
    SrcTgtColumnMapping = dict(zip(AllAttributes, RenameTgtAttributes))
    targetDF = targetDF.select([col(column).alias(SrcTgtColumnMapping.get(column, column)) for column in targetDF.columns])
    StepNo = 1

    # Step 2: Creating DataFrame for Source Data Set
    sourceDF = (SourceDataDF
                .select(*AllAttributes
                        , xxhash64(*TrackingAttributes).alias("source_hash")
                        , concat_ws('|', *KeyAttributes).alias("source_MERGEKEY"))
                .drop("ActiveFlag", "EffectiveStartDate", "EffectiveEndDate")
               )
    StepNo = 2

    # Step 3: Joining Source with Target Dataset & identifying Record Type
    KeyAttributes_tgt = ["tgt_" + column for column in KeyAttributes]

    joinDF = (sourceDF.join(targetDF, ([col(lhsCol) == col(rhsCol) for (lhsCol, rhsCol) in zip(KeyAttributes, KeyAttributes_tgt)]), "fullouter")
              .select(sourceDF["*"], targetDF["*"]
                      , when((targetDF.tgt_ActiveFlag == "Y") & (sourceDF.source_hash == targetDF.target_hash), "NO ACTION")
                      .when((targetDF.tgt_ActiveFlag == "D") & (sourceDF.source_MERGEKEY.isNull()), "NO ACTION")
                      .when((targetDF.tgt_ActiveFlag == "Y") & (sourceDF.source_MERGEKEY.isNull()), "DELETE")
                      .when((targetDF.tgt_ActiveFlag == "D") & (sourceDF.source_MERGEKEY.isNotNull()), "UPSERT")
                      .when(targetDF.target_MERGEKEY.isNull(), "INSERT")
                      .otherwise("UPSERT").alias("RecordTypeFlag"))
             )
    StepNo = 3

    # Step 4: Identifying/Segregating Records available in Target but not received in Source
    DeleteRecordsDF = joinDF.filter("RecordTypeFlag == 'DELETE'")
    StepNo = 4

    # Step 5: Creating DataFrame tfor Inactivating Records for which a Change is received in source
    InactivateRecordsDF = joinDF.filter("RecordTypeFlag == 'UPSERT'")
    StepNo = 5

    # Step 6: Identifying/Segregating Changed/New records for Inserting into Target
    InsertRecordsDF = (joinDF.filter("RecordTypeFlag == 'UPSERT' OR RecordTypeFlag == 'INSERT'")
                       .withColumn("EffectiveStartDate", current_date())
                       .withColumn("EffectiveEndDate", to_date(lit('9999-12-31')))
                       .withColumn("ActiveFlag", lit('Y'))
                       .select(*AllAttributes)
                      )
    StepNo = 6

    # Step 7: Executing Merge Statement on Target to flag "NOT RECEIVED" Records as "D"
    targetTable = DeltaTable.forName(spark, TargetTableName)

    # Preparing Join Condition for Merge Statement
    MergeJoinCondition = "concat_ws('|', "
    MergeJoinCondition = MergeJoinCondition + "".join(["target." + column + ", " for column in KeyAttributes])

    # Deriving Version No of Table before Load
    TableVersionNoBeforeLoad = spark.sql(f"""
      SELECT * FROM (DESCRIBE HISTORY {TargetTableName}) TEMP ORDER BY VERSION DESC
    """).collect()[0][0]
    print(f"[INFO]: Version No. of Table [{TargetTableName}] before Data Load: [{TableVersionNoBeforeLoad}].")

    if DeleteRecordsDF.count() > 0:
      targetTable.alias("target").merge(
        source = DeleteRecordsDF.alias("source"),
        condition = MergeJoinCondition[:-2] + ") = source.target_MERGEKEY and ActiveFlag = 'Y'" + f"AND target.MarketUnitName == '{MarketUnit}'"
      ).whenMatchedUpdate(set =
                          {
                            "UpdatedTime": "current_timestamp",
                            "EffectiveEndDate": "current_date",
                            "ActiveFlag": "'D'"
                          }
      ).execute()
      StepNo = 7

    # Step 8: Executing Merge Statement on Target to Inactivate existing records for which a Changed Record  is received
    if InactivateRecordsDF.count() > 0:
      targetTable.alias("target").merge(
        source = InactivateRecordsDF.alias("source"),
        condition = MergeJoinCondition[:-2] + ") = source.source_MERGEKEY and (ActiveFlag = 'Y' or ActiveFlag = 'D')" + f"AND target.MarketUnitName == '{MarketUnit}'"
      ).whenMatchedUpdate(set =
                          {
                            "UpdatedTime": "current_timestamp",
                            "EffectiveEndDate": "current_date",
                            "ActiveFlag": "'N'"
                          }
      ).execute()
      StepNo = 8

    # Step 9: Inserting New and Changed Records
    if InsertRecordsDF.count() > 0:
      TargetTablePath = spark.sql(f"describe detail {TargetTableName}").collect()[0]['location']  
      InsertRecordsDF.write.format("delta").partitionBy("MarketUnitName").mode("append").save(f'{TargetTablePath}')
    StepNo = 9

    spark.sql("OPTIMIZE " + TargetTableName + " ;")

    print(f"[INFO]: Loading of Table [{TargetTableName}] Completed Successfully.")

  except Exception as error:
    print("[ERROR]: Failed While Loading Table [", TargetTableName, "] !!!", error)
    nbpath = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    error_string = str(error)
    spark.sql(f"""Insert into {audit_db}.softexceptionlog values ('{nbpath}','MosaicCommonUtilities','SCDType2DataLoad',from_utc_timestamp(current_timestamp,'US/Eastern'),'{market}',"{error_string}");""")
    
    if StepNo == 7 or StepNo == 8:
      print("[ERROR]: Failed while performing Step 8 (Inactivating Existing Records) or Step 9 (Inserting New/Changed Records) !!!")
      print(f"[INFO]: Restoring Table [{TargetTableName}] to Previous Version [{TableVersionNoBeforeLoad}] ...")
      spark.sql(f"RESTORE TABLE {TargetTableName} TO VERSION AS OF {TableVersionNoBeforeLoad}")
      print(f"[INFO]: Table [{TargetTableName}] Restored to Previous Version Successfully.")
      raise error

    elif StepNo == 9:
      print(f"[INFO]: OPTIMIZE {TargetTableName} Command Failed. Restoring of Table to Previous Version is not required. No Action Needed !")

    else:
      raise error

# COMMAND ----------

def prepareCsvAttachmentgeneric(df):
    try:
        df = df.select(*[col(colm).cast('string').alias(colm) if typ == 'timestamp' else col(colm) for colm, typ in df.dtypes])
        for colm in df.columns:
            df = df.withColumn(colm,regexp_replace(col(colm),',',' '))
        df = df.withColumn(df.columns[0],concat(lit('\n'),col(df.columns[0])))
        df = df.withColumn(df.columns[0],when(col(df.columns[0]).isNull(),concat(lit('\n'),lit(None).cast("string"))).otherwise(col(df.columns[0])))
        pdf = df.toPandas()
        fdf1 = pdf.reset_index(drop='index').fillna('NA')
        val_lst = fdf1.astype(str).values.flatten().tolist()
        final_lst = df.columns + val_lst
        attachment_data = ','.join(final_lst)
        return attachment_data
    except Exception as e:
        return 'No Data'

# COMMAND ----------

def sendEmailNotificationgeneric(content, receiver, subject, message, attachmentname):
    from datetime import datetime
    try:
        env_details_dict ={'cdomosaicdevblob':'DEV','cdomosaicqablob':'QA','cdomosaicpreprodblob':'PRE-PROD','cdomosaicprodblob':'PROD'}
        env_details = env_details_dict[MosaicBlob]
        truncated_msg = ''
        now = datetime.now()
        attachmentContent = content
        emailJSON = {'receiver': receiver,
                 'subject':subject,
                 'message':f'{message} {env_details} env executed at {now} UTC',
                 'EmailType': 'mosaic_dq',
                 'attachmentName':attachmentname+'.csv',
                 'attachmentContent':attachmentContent}
        logic_app_post_url = dbutils.secrets.get(scope=adbScope ,key="LogicAppURL")
        response = requests.post(logic_app_post_url, json=emailJSON)
        return response
    except Exception as e:
        print(e)
        return 'failed to send email' 

# COMMAND ----------

# MAGIC %md #### Function for deriving Record Counts after the data load to Delta Table

# COMMAND ----------

def GetDeltaTableLoadStats(TableName):
  """
    This function retrieves # of records Inserted/Updated/Deleted of Delta Table after the data load.

    Input: Fully Qualified TableName
    Output: # of Records Inserted, # of Records Updated & # of Records Deleted
  """

  # Retrieving Table History for a particular data load
  ClusterId = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
  NotebookJobUrl = au.get_notebook_job_url()
  RunId = NotebookJobUrl.split("/")[-1]

  TableHistoryOutput = spark.sql(f"""
  SELECT operation, operationMetrics, job, clusterId
  FROM (DESCRIBE HISTORY {TableName}) tmp
  WHERE clusterId = '{ClusterId}'
  AND operation IN ('WRITE', 'MERGE', 'UPDATE', 'DELETE')
  ORDER BY timestamp ASC
  """).collect()

  # Deriving the Inserted/Updated/Deleted Record Counts
  numInsertedRows = 0
  numUpdatedRows = 0
  numDeletedRows = 0

  for Metrics in TableHistoryOutput:
    if (Metrics[2]['runId'] == RunId and Metrics[0] == 'WRITE'):
      numInsertedRows = numInsertedRows + 0 if Metrics[1].setdefault('numOutputRows', None) == None else int(Metrics[1]['numOutputRows'])

    elif (Metrics[2]['runId'] == RunId and Metrics[0] == 'MERGE'):
      numInsertedRows = numInsertedRows + int(Metrics[1]['numTargetRowsInserted'])
      numUpdatedRows = numUpdatedRows + int(Metrics[1]['numTargetRowsUpdated'])
      numDeletedRows = numDeletedRows + int(Metrics[1]['numTargetRowsDeleted'])

    elif (Metrics[2]['runId'] == RunId and Metrics[0] == 'UPDATE'):
      numUpdatedRows = numUpdatedRows + int(Metrics[1]['numUpdatedRows'])

    elif (Metrics[2]['runId'] == RunId and Metrics[0] == 'DELETE'):
      numDeletedRows = numDeletedRows + int(Metrics[1]['numDeletedRows'])

  print(f"[INFO]: Data Load Statistics for Table: {TableName}")
  print(f"[INFO]: Inserted Record Count: {numInsertedRows}")
  print(f"[INFO]: Updated Record Count: {numUpdatedRows}")
  print(f"[INFO]: Deleted Record Count: {numDeletedRows}")

  return numInsertedRows, numUpdatedRows, numDeletedRows

# COMMAND ----------

# MAGIC %md ## Misc Utilities

# COMMAND ----------

from datetime import date


def get_previous_period(load_periods):
    """Calculates previous year and month for load period.
    
    If multiple periods specified, calculate previous period for oldest one.
    """
    sorted_load_periods = sorted([int(x) for x in load_periods])
    oldest_year, oldest_month = (int(str(sorted_load_periods[0])[0:4]), int(str(sorted_load_periods[0])[4:6]))

    previous_year, previous_month = (oldest_year, oldest_month - 1) if oldest_month != 1 else (oldest_year - 1, 12)

    previous_year = str(previous_year)
    previous_month = '0' + str(previous_month) if previous_month < 10 else str(previous_month)

    return previous_year, previous_month

# COMMAND ----------

from datetime import datetime


def apply_date_filter(df, filter_column, load_type, load_range=None):
    """Apply date filter to given dataframe.

    Date range can be year-month combination or year-period combination.

    Args:
        - df: A Spark DataFrame to apply filter
        - filter_column: Name of column to use in filter
        - load_type: delta (take current month - 1), adhoc (for specified range)
                     history (all data)
        - load_range: filter value in format '{YYYYmm}+' or 'YYYYppp'

    Returns:
        - A Spark DataFrame with applied filter transformation

    Raises:
        - Exception if load_type=adhoc and load_range is None
        - ValueError if load_type doesn't equal to adhoc, delta or history 
    """
    if load_type.lower() == 'adhoc':
        if not load_range:
            raise Exception('Widget parameter load_yearmonth should be set for adhoc load')

        load_range = list({x.strip() for x in load_range.split(',')})
        
        load_range_formatted = []
        df_result = df
        for period in load_range:
            year = period[0:4]
            month = period[5:]

            # Add leading zero to 1-9 month
            month = '0' + month if len(month) == 1 else month

            df_result = df_result.filter(col('DocumentPostingDate').like(f'{year}-{month}%'))

            load_range_formatted.append(f'{year}-{month}')
        
        print(f"Load type is '{load_type}' for {load_range_formatted}")
        df_result = df.filter(col(filter_column).like(f'{period_year}-{period_month}%'))
    elif load_type.lower() == 'delta':
        current_year = str(datetime.today().year)
        current_month = str(datetime.today().month)
        
        # Add leading zero to 1-9 month
        current_month = '0' + current_month if len(current_month) == 1 else current_month

        previous_year, previous_month = get_previous_period([current_year + current_month])

        print(f"Load type is '{load_type}' for {previous_year}-{previous_month}")
        df_result = df.filter(col('DocumentPostingDate').like(f'{previous_year}-{previous_month}%'))
    elif not load_type or load_type.lower() == 'history':
        print(f"Load type is '{load_type}'")

        df_result = df
    else:
        raise ValueError(f'Wrong value for widget parameter load_type: {load_type}')

    return df_result

# COMMAND ----------

def replace_non_breaking_space_character(df, trim_flag=True):
    """
    It replace non-breaking spaces for blank spaces for the provided dataframe. It usally come from manual input files 

    args:
     - df : PySpark Dataframe - Dataframe that we want to clean the non-breaking space to blank space
     - trim_flag : Boolean - by default it clean trailing spaces

    return :
     - PySpark Dataframe

    """
    string_columns = [_c[0] for _c in df.dtypes if _c[1]=='string']

    for _column in string_columns:
        df = df.withColumn(_column, regexp_replace(col(_column), '[\\u00A0]', ' '))
    
    if trim_flag:
        for _column in string_columns:
            df = df.withColumn(_column, trim(col(_column)))
    
    return df

# COMMAND ----------

# MAGIC %md ## Executing Stored Procedure to Move data from Synapse Stage table to Synapse Main Table

# COMMAND ----------

def SynapseStgtoMainTableLoad(SrcTblName, TgtTblName, Market):
  """ This function is used to load the data from Synapse stage table to Synapse target table based on the periods available in synapse stage table. The functions accepts 3 arguments. Stage table name, """
  statement = f"""EXEC etl_mosaic.SynapseStgtoMainTableLoad '{SrcTblName}', '{TgtTblName}', '{Market}'"""
  print(statement)
  if unity_flag.upper() == "Y":
    tokenstruct = buildSynapseToken()

    conn_str = (
      "DRIVER={ODBC Driver 18 for SQL Server};"
      f'SERVER=tcp:{dwServer},1433;'
      f'DATABASE={dwDatabase};'
      "Encrypt=yes;"
      "TrustServerCertificate=no;"
    )

    SQL_COPT_SS_ACCESS_TOKEN = 1256
    with pyodbc.connect(conn_str,
                        attrs_before = {SQL_COPT_SS_ACCESS_TOKEN:tokenstruct},
                        autocommit=True) as conn:

      with conn.cursor() as cursor:
          cursor.execute(statement)
  else:
    con = SynapseConnection()
    exec_statement = con.prepareCall(statement)
    exec_statement.execute()


    # Close connections
    exec_statement.close()
    con.close()

# COMMAND ----------

# MAGIC %md ## Common ETL functions

# COMMAND ----------

def make_tiger_skus(df_in, 
                    df_sku_masterdata, 
                    in_sku_column='MaterialId', 
                    masterdata_sku_column='SellingSkuCode'
                    ):
    """Append leading 'T' to Tiger SKUs based on masterdata.

    This function modifies input dataframe with adding leading
    'T' character to all SKUs which have T prefix in Product Masterdata.

    The flow:
        1. Filter masterdata dataframe by SKUs starting with T
        2. Remove leading T from Tiger SKUs
        3. Match Tiger SKUs from masterdata with input dataframe
        4. Form matched records, add leading T to SKU

    Args:
      - df_in:                 A Spark DataFrame with input data to modify
      - df_sku_masterdata:     A Spark DataFrame with SKUs masterdata to identify
                               Tiger SKUs
      - in_sku_column:         A column name in input dataframe indicating SKU
      - masterdata_sku_column: A column name in masterdata dataframe 
                               indicating SKU
    
    Returns:
      - A modified Spark DataFrame with Tiger SKUs having leading T character
    """
    df_masterdata_tiger_sku = (
        df_product
        .filter(col(masterdata_sku_column).startswith('T'))
        .withColumn('__SKU_COLUMN__', regexp_replace(col(masterdata_sku_column), r'^T', ''))
        .select('__SKU_COLUMN__', masterdata_sku_column)
    )

    df_in_to_tiger_sku = (
    df_in
        .join(
            other=df_masterdata_tiger_sku,
            on=df_in[in_sku_column] == df_masterdata_tiger_sku['__SKU_COLUMN__'],
            how='left'
        )
    )

    df_with_tiger_skus = (
        df_in_to_tiger_sku
        .withColumn(in_sku_column, coalesce(masterdata_sku_column, in_sku_column))
        .drop('__SKU_COLUMN__', masterdata_sku_column)
    )

    return df_with_tiger_skus

# COMMAND ----------

# MAGIC %md
# MAGIC ### Version Comparison Function

# COMMAND ----------

#Function to fetch schema of a particular table and version
def fetch_schema(tableName, version):
    try:
        schema_df = spark.read.option("versionAsOf", version).table(tableName)
        return schema_df.schema
    except Exception as e:
        raise
#Function to compare the versions
def compare_versions(tableName, version1, version2, filterCondition, IgnoreColumns, IncludeColumns):
    try:
        diff_count = (spark.read.option("versionAsOf", version1).table(tableName)
                      .filter(filterCondition).drop(*IgnoreColumns).selectExpr(*IncludeColumns if IncludeColumns else "*")
                      .exceptAll(spark.read.option("versionAsOf", version2).table(tableName)
                                 .filter(filterCondition).drop(*IgnoreColumns).selectExpr(*IncludeColumns if IncludeColumns else "*"))
                      .count())
        return diff_count
    except Exception as e:
        raise

#Function to check schema and DataTypes 
def check_schema_and_datatype_compatibility(tableName, version1, version2):
    try:
        schema1 = fetch_schema(tableName, version1)
        schema2 = fetch_schema(tableName, version2)
        schemaMatch = schema1 == schema2
        return schemaMatch
    except Exception as e:
        return False
    
def getVersionDifferenceCount(workflowName:str, marketUnitName:str, dbName:str, tableName:str, lastReadVersion:list, IgnoreColumns:list, IncludeColumns:list, filterCondition:str, VersionComparisonFlag:bool) -> dict:

    """
    *** 1. Pass the parameters workflowName, marketUnitName, dbName, tableName, lastReadVersion, IgnoreColumns, IncludeColumns, filterCondition.
    *** 2. Compare the current version with the previous version of the table.
    *** 3. Check if there are any schema or datatype mismatch if the table already exists.
    *** 4. Based on the checks mentioned above updating the tableDiffCount variable value.
    """ 
    
    import datetime
    dbtableName = f"{dbName}.{tableName}"

    # Get the Last 2 version details
    latestVersionNumbers = spark.sql(f"SELECT version,timestamp as modifieddate FROM (DESCRIBE HISTORY {dbtableName} ) T ORDER BY VERSION DESC LIMIT 1").collect()

    # Add the last read version if provided
    latestVersionNumbers.extend(lastReadVersion) if lastReadVersion else None

    # Default value: -1 -> record present in the table, -2 -> both versions are same , -3 -> schema changes, -4 -> Version Comparison Skipped, 0 -> No count difference between the two versions
    tableCountDiff = -4

    if VersionComparisonFlag:
        tableCountDiff = -1
        if len(latestVersionNumbers) >= 2:
            version1 = latestVersionNumbers[0]["version"]
            version2 = latestVersionNumbers[1]["version"]
            if version1 == version2:
                tableCountDiff = -2

            else:
                # Check schema and data type compatibility before comparing data
                schemaMatch = check_schema_and_datatype_compatibility(dbtableName, version1, version2)

                if schemaMatch:
                    tableCountDiff = compare_versions(dbtableName, version1, version2, filterCondition, IgnoreColumns, IncludeColumns)
                else:
                    tableCountDiff = -3

    return {
        "WorkFlowName": workflowName,
        "TableName": dbtableName,
        "VersioNo": latestVersionNumbers[0]["version"],
        "VersionTimeStamp": latestVersionNumbers[0]["modifieddate"],
        "MarketUnitName": marketUnitName,
        "tableCountDiff": tableCountDiff
    }

# COMMAND ----------

def table_exists(*args):
    """
    This function checks if a table exists
    It is used in place of 'spark._jsparkSession.catalog().tableExists' which is not whitelisted to be used on Unity Catalog Shared Clusters

    Parameters:
    - *args: str
        - If one argument is passed, it must be in the format of 'database.table'
        - If two arguments are passed, it must be in the format of 'database', 'table'

    Returns:
    - bool: 
        - Returns True if the table exists, otherwise False
    """

    if len(args) == 1:
        if '.' in args[0]:
            database, table = args[0].split('.')
        else:
            raise ValueError("If passing one argument, the expected parameter format should be in 'database.table' format")

    elif len(args) == 2:
        if '.' in args[0] or '.' in args[1]:
            raise ValueError("If passing two arguments, 'database' and 'table' should be passed separately and not as fully qualified table names")

        database, table = args
    else:
        raise ValueError("Invalid number of arguments.  Pass either 'database.table' or 'database' and 'table'")


    query = f"SHOW TABLES IN {database}"
    query_df = spark.sql(query)
    query_df = query_df.filter((lower(query_df['tableName']) == table.lower()) & (query_df['isTemporary'] == False))

    return query_df.count() > 0

# COMMAND ----------

# MAGIC %md
# MAGIC ### DBSQL Serverless Functions

# COMMAND ----------

def WorkflowStatusLogInsert(WfName, Market):
  try:
    statement = f'''
              INSERT INTO uc_{env}_snt_mosaic_01.etl_mosaic.WorkflowStatusLogs 
                (BatchId,
                Market,
                SystemId,
                WorkflowName,
                WorkflowType,
                TableName,
                RunFrequencyInMinutes,
                WorkflowStartDateTimeInUTC,
                WorkflowStatus,
                WorkflowEndDateTimeInUTC,
                SynapseLoadStartDateTimeInUTC,
                SynapseLoadStatus,
                SynapseLoadEndDateTimeInUTC,
                TM1StartDateTimeInUTC,
                TM1Status,
                TM1EndDateTimeInUTC
                )
                SELECT BatchId,
                Market,
                Sys_ID,
                WF_Name,
                PipeLineType,
                Synapse_Table,
                FreqTime,
                try_to_timestamp(ADFStartDateTimeInUTC, 'MMM dd yyyy hh:mm:ss:SSSa'),
                ADFStatus,
                try_to_timestamp(ADFEndDateTimeInUTC, 'MMM dd yyyy hh:mm:ss:SSSa'),
                try_to_timestamp(WFStartDateTime, 'MMM dd yyyy hh:mm:ss:SSSa'),
                WFStatus,
                try_to_timestamp(WFEndDateTime, 'MMM dd yyyy hh:mm:ss:SSSa'),
                try_to_timestamp(TM1StartDateTime, 'MMM dd yyyy hh:mm:ss:SSSa'),
                TM1Status,
                try_to_timestamp(TM1EndDateTime, 'MMM dd yyyy hh:mm:ss:SSSa')
                FROM uc_{env}_snt_mosaic_01.etl_mosaic.DataTransmission_Control_Table WHERE WF_Name = "{WfName}" AND Market = "{Market}"
            '''
    
    # DBSQL Serverless Connection Setup
    import databricks.sql
    conn = databricks.sql.connect(
        server_hostname=dbsql_server,
        http_path=dbsql_http_path,
        access_token=dbsql_accesstoken
    )

    cursor = conn.cursor()
    cursor.execute(statement)
    print(cursor.fetchall())
    
  except Exception as e:
    print(f"WorkflowStatusLogInsert Failed {0}".format(e))

# COMMAND ----------

def WorkflowRunStatusUpdate(WfName, Market, RuleRunId, RunStatus):
  try:
    """This function updates the Synapse data load status in the control table"""
    if RunStatus == 'WorkflowRunStart':
      statement = f'''UPDATE uc_{env}_snt_mosaic_01.etl_mosaic.DataTransmission_Control_Table 
                      SET ADFStartDateTimeInUTC = date_format(from_utc_timestamp(current_timestamp(), 'UTC')
                      , 'MMM dd yyyy hh:mm:ss:SSSa')
                      , ADFStatus = 'R'
                      , BatchId = {RuleRunId} 
                      , ADFEndDateTimeInUTC = null
                      , WFStartDateTime= null 
                      , WFStatus= null
                      , WFEndDateTime = null
                      where WF_Name = "{WfName}" and Market = "{Market}"''' 
    
    elif RunStatus == 'WorkflowRunSuccess':
      statement = f'''UPDATE uc_{env}_snt_mosaic_01.etl_mosaic.DataTransmission_Control_Table
                      SET ADFEndDateTimeInUTC = date_format(from_utc_timestamp(current_timestamp(), 'UTC'), 'MMM dd yyyy hh:mm:ss:SSSa'), ADFStatus = 'S'
                      WHERE WF_Name = "{WfName}" and Market = "{Market}"''' 
    
    elif RunStatus == 'WorkflowRunError':
      statement = f'''UPDATE uc_{env}_snt_mosaic_01.etl_mosaic.DataTransmission_Control_Table 
                      SET ADFEndDateTimeInUTC = date_format(from_utc_timestamp(current_timestamp(), 'UTC'), 'MMM dd yyyy hh:mm:ss:SSSa'), ADFStatus = 'E' 
                      WHERE WF_Name = "{WfName}" and Market = "{Market}"'''
    
    # DBSQL Serverless Connection Setup
    import databricks.sql
    conn = databricks.sql.connect(
        server_hostname=dbsql_server,
        http_path=dbsql_http_path,
        access_token=dbsql_accesstoken
    )
    cursor = conn.cursor()
    cursor.execute(statement)
    print(cursor.fetchall())
  except Exception as e:
    print(f"WorkflowRunStatusUpdate Failed {0}".format(e))

# COMMAND ----------

def execute_sql_statement(statement: str):
    print("[INFO] Executing using DBSQL Serverless Connection")
    import databricks.sql

    conn = databricks.sql.connect(
        server_hostname=dbsql_server,
        http_path=dbsql_http_path,
        access_token=dbsql_accesstoken
    )

    cursor = conn.cursor()
    cursor.execute(statement)
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return result

# COMMAND ----------

def TM1CurrentStatusCheckDBSQL(Gold_Table, MarketName):
    query_count_tm1 = f"""
        SELECT COUNT(*) as cnt
        FROM uc_{env}_snt_mosaic_01.etl_mosaic.datatransmission_control_table
        WHERE WF_Name = (
            SELECT WF_Name
                FROM (
                    SELECT WF_Name,
                        ROW_NUMBER() OVER (PARTITION BY Gold_Table ORDER BY ADFStartDateTimeInUTC DESC) AS row_num
                    FROM uc_{env}_snt_mosaic_01.etl_mosaic.datatransmission_control_table
                    WHERE LOWER(Gold_Table) = LOWER('{Gold_Table}')
                    AND Market = '{MarketName}'
                    AND ADFStatus IN ('R')
                ) tmp
                WHERE row_num = 1
        )
        AND Market = '{MarketName}'
        AND LOWER(Gold_Table) = LOWER('{Gold_Table}')
        AND TM1Status IN ('R')
    """
    print(query_count_tm1)
    try:
        df_count = execute_sql_statement(query_count_tm1)
        active_n_error_tables_count = df_count[0][0]
        
        print(f"Active/Error Table Count: {active_n_error_tables_count}")
        return active_n_error_tables_count
    except Exception as e:
        print(f"Error executing TM1 status check query: {str(e)}")
        return -1

# COMMAND ----------

# def TM1LoadWaitDBSQL(Gold_Table, MarketName):
#   """This function implements waiting till all the tables have been marked as success for a specific workflow before beginning data load"""
#   ActiveNErrorTablesCount = TM1CurrentStatusCheckDBSQL(Gold_Table, MarketName)
#   while ActiveNErrorTablesCount != 0:
#     print(f"TM1 table load is in progress. Waiting for 60 seconds before retrying data load...")
#     time.sleep(60) 
#     ActiveNErrorTablesCount = TM1CurrentStatusCheckDBSQL(Gold_Table, MarketName)
    
#     if ActiveNErrorTablesCount == 0:   #fail-proof exit
#       ActiveNErrorTablesCount = 0

# COMMAND ----------

import time
def TM1LoadWaitDBSQL(Gold_Table, MarketName):
    """This function implements waiting till all the tables have been marked as success for a specific workflow before beginning data load"""
    max_retries = 30
    retry_count = 0

    ActiveNErrorTablesCount = TM1CurrentStatusCheckDBSQL(Gold_Table, MarketName)
    
    while ActiveNErrorTablesCount != 0 and retry_count < max_retries:
        print(f"TM1 table load is in progress. Waiting for 60 seconds before retrying data load... (Attempt {retry_count + 1}/{max_retries})")
        time.sleep(60) 
        retry_count += 1
        ActiveNErrorTablesCount = TM1CurrentStatusCheckDBSQL(Gold_Table, MarketName)
    
    if ActiveNErrorTablesCount == 0:
        print("TM1 table load completed successfully!")
    else:
        print(f"TM1 table load status check exited after {max_retries} retries")
        # Email notification future scope
        # error_msg = f"TM1 table load failed after {max_retries} retries. Gold_Table: {Gold_Table}, MarketName: {MarketName}, Remaining error count: {ActiveNErrorTablesCount}"
        # print(error_msg)
        # raise RuntimeError(error_msg)

# COMMAND ----------

def DBSQLRunStatusUpdate(Gold_Table, Market, RunStatus):
  try:
    """This function updates the Synapse data load status in the control table"""
    if RunStatus == 'DBSQLRunStart':
      statement = f'''UPDATE uc_{env}_snt_mosaic_01.etl_mosaic.DataTransmission_Control_Table 
                      SET WFStartDateTime = date_format(from_utc_timestamp(current_timestamp(), 'UTC'), 'MMM dd yyyy hh:mm:ss:SSSa'), WFStatus= 'R'
                      where LOWER(Gold_Table) = LOWER("{Gold_Table}") and Market = "{Market}"''' 
    
    elif RunStatus == 'DBSQLRunSuccess':
      statement = f'''UPDATE uc_{env}_snt_mosaic_01.etl_mosaic.DataTransmission_Control_Table
                      SET WFEndDateTime = date_format(from_utc_timestamp(current_timestamp(), 'UTC'), 'MMM dd yyyy hh:mm:ss:SSSa'), WFStatus = 'S'
                      WHERE LOWER(Gold_Table) = LOWER("{Gold_Table}") and Market = "{Market}"''' 
    
    elif RunStatus == 'DBSQLRunError':
      statement = f'''UPDATE uc_{env}_snt_mosaic_01.etl_mosaic.DataTransmission_Control_Table 
                      SET WFEndDateTime = date_format(from_utc_timestamp(current_timestamp(), 'UTC'), 'MMM dd yyyy hh:mm:ss:SSSa'), WFStatus = 'E' 
                      WHERE LOWER(Gold_Table) = LOWER("{Gold_Table}") and Market = "{Market}"'''
    
    # DBSQL Serverless Connection Setup
    import databricks.sql
    conn = databricks.sql.connect(
        server_hostname=dbsql_server,
        http_path=dbsql_http_path,
        access_token=dbsql_accesstoken
    )
    cursor = conn.cursor()
    cursor.execute(statement)
    print(cursor.fetchall())
  except Exception as e:
    print(f"WorkflowRunStatusUpdate Failed {0}".format(e))

# COMMAND ----------

# %pip install databricks-labs-dqx
%pip install databricks-labs-dqx


# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx import *
from databricks.sdk import WorkspaceClient


print("✅ Imports successful!")

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql import SparkSession
from databricks.labs.dqx.engine import DQEngine

# COMMAND ----------

# MAGIC %pip uninstall -y databricks-labs-dqx
# MAGIC %pip install databricks-labs-dqx
# MAGIC

# COMMAND ----------

# ✅ Full Example: Sample DataFrame → Profile → Apply Quality Checks → Inspect Valid & Quarantined

from pyspark.sql import SparkSession
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.engine import *
from databricks.labs.dqx import *
from databricks.sdk import WorkspaceClient  # for Databricks
import random, string

# 1. _START SPARK_
spark = SparkSession.builder.appName("DQX_SampleDemo").getOrCreate()

# 2. _CREATE SAMPLE DATA (100 records) with intentional quality issues_
def random_name():
    return ''.join(random.choices(string.ascii_letters, k=5))

data = []
for i in range(1, 101):
    row = {
        "id": i,
        "name": random_name() if random.random() > 0.1 else None,  # ~10% null
        "age": random.randint(10, 70),
        "salary": random.choice([random.randint(-1000, 10000), None])  # some negative or null
    }
    data.append(row)

df = spark.createDataFrame(data)
print("🧪 Sample DataFrame")
df.show(5, truncate=False)

# # 3. _PROFILE DATA for rule suggestions (optional)_
# profiler = DQProfiler()
# summary, profiles = profiler.profile(df)
# print("🔍 Summary Stats"); summary.show()
# Optionally inspect `profiles` for each column characteristics

# 4. _DEFINE QUALITY RULES_
checks = [
    DQCheck(column="id", description="id not null", expression="id IS NOT NULL", action=DQAction.QUARANTINE),
    DQCheck(column="name", description="name not null", expression="name IS NOT NULL", action=DQAction.QUARANTINE),
    DQCheck(column="age", description="age >= 18", expression="age >= 18", action=DQAction.QUARANTINE),
    DQCheck(column="salary", description="salary >= 0", expression="salary >= 0", action=DQAction.QUARANTINE),
]

# 5. _INITIALIZE DQX ENGINE_ (auto-auth on Databricks)
dq_engine = DQEngine(WorkspaceClient())

# 6. _APPLY RULES AND SPLIT_
valid_df, quarantine_df = dq_engine.apply_checks_by_metadata_and_split(df, checks)

# 7. _INSPECT RESULTS_
print(f"✅ Valid records: {valid_df.count()}")
print(f"❌ Quarantined records: {quarantine_df.count()}")
quarantine_df.show(truncate=False)

# 8. _USE RESULTS_
# valid_df.write.parquet(...)
# quarantine_df.write.parquet(...)

# 9. _STOP SPARK (optional)_
spark.stop()


# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx import *
from databricks.sdk import WorkspaceClient

# Initialize the WorkspaceClient to interact with the Databricks workspace
ws = WorkspaceClient()

# Initialize the DQEngine with the workspace client
dq_engine = DQEngine(ws)

# Define data quality checks
checks = [
    DQColRule(
        name="age_is_valid",
        criticality="error",
        check_func=is_in_range,
        col_name="age",
        check_func_args=[18, 65]
    ),
    DQColSetRule(
        columns=["salary", "bonus"],
        criticality="warn",
        check_func=is_not_null
    )
]

# Apply the checks to your DataFrame
input_df = spark.read.table("your_table_name")
valid_df, quarantined_df = dq_engine.apply_checks_and_split(input_df, checks)

# Show the results
valid_df.show()
quarantined_df.show()


# COMMAND ----------

import databricks.labs.dqx
print(databricks.labs.dqx.__version__)

# COMMAND ----------

import databricks.labs.dqx.rule as rule
print(dir(rule))

# COMMAND ----------

# MAGIC %pip install --upgrade databricks-labs-dqx

# COMMAND ----------

# MAGIC %restart_python 

# COMMAND ----------

import requests

url = "https://friday-stallion-app.azurewebsites.net/api/http_trigger_stallion"
params = {"code": "M0MxT-qJDnVTMTQHzr67KIngSBaXfIY94AVFuveNdB3yAzFuxO4Dlg=="}
data = {"name": "Stallion", "age": 5}

response = requests.post(url, params=params, json=data)
print(response.text)

# COMMAND ----------

import requests
import json

# Your Logic App HTTP endpoint URL
logic_app_url = "https://prod-60.eastus.logic.azure.com:443/workflows/e1abef439b424951b3736200095fb144"

# Define your payload
payload = {
    "email": "tjeshwanth17@gmail.com",
    "subject": "Databricks Triggered Email",
    "body": "Hello! This email was triggered from Databricks using a Logic App."
}

# Send the POST request
response = requests.post(logic_app_url, data=json.dumps(payload), headers={"Content-Type": "application/json"})

# Check response
if response.status_code == 200:
    print("Email sent successfully!")
else:
    print(f"Failed to send email: {response.status_code}, {response.text}")


# COMMAND ----------

# DBTITLE 1,working scenario
import requests
import json

logic_app_url = "https://prod-60.eastus.logic.azure.com:443/workflows/e1abef439b424951b3736200095fb144/triggers/When_a_HTTP_request_is_received/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2FWhen_a_HTTP_request_is_received%2Frun&sv=1.0&sig=bwubT1lwvmYot9TgFKKc2yTLzJ16KIFH5pIi7W7MfUg"

payload = {
    "email": "tjeshwanth17@gmail.com;saipraneeth.nan@gmail.com",
    "subject": "Databricks Triggered Email",
    "body": "Hello! This email was triggered from Databricks using a Logic App."
}

response = requests.post(logic_app_url, json=payload)

if response.status_code == 200:
    print("Email sent successfully!")
else:
    print(f"Failed to send email: {response.status_code}, {response.text}")


# COMMAND ----------

tenant_id="60419507-249c-412c-bf2e-a187855acd2f",  # Ensure this is in the correct format
client_id="15e6792e-c481-462a-bc6b-c402732dcc39",
client_secret="Qbp8Q~Y9~h18-dGi5iz38hgeRwBS1oxfWAXn2c0s",

# COMMAND ----------

import azure.functions as func
import requests
from msal import ConfidentialClientApplication

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        recipient = req_body.get('to')
        subject = req_body.get('subject')
        body = req_body.get('body')

        tenant_id = "60419507-249c-412c-bf2e-a187855acd2f"
        client_id = "15e6792e-c481-462a-bc6b-c402732dcc39"
        client_secret = "Qbp8Q~Y9~h18-dGi5iz38hgeRwBS1oxfWAXn2c0s"
        sender_email = "saipraneeth.nan@gmail.com"

        authority = f"https://login.microsoftonline.com/{tenant_id}"
        scope = ["https://graph.microsoft.com/.default"]

        app = ConfidentialClientApplication(client_id, authority=authority, client_credential=client_secret)
        token = app.acquire_token_for_client(scopes=scope)

        if "access_token" not in token:
            return func.HttpResponse(f"Authentication failed: {token.get('error_description')}", status_code=401)

        headers = {
            "Authorization": f"Bearer {token['access_token']}",
            "Content-Type": "application/json"
        }

        email_msg = {
            "message": {
                "subject": subject,
                "body": {
                    "contentType": "Text",
                    "content": body
                },
                "toRecipients": [
                    {
                        "emailAddress": {
                            "address": recipient
                        }
                    }
                ]
            },
            # Optional: "saveToSentItems": "false"
        }

        send_url = f"https://graph.microsoft.com/v1.0/users/{sender_email}/sendMail"
        response = requests.post(send_url, headers=headers, json=email_msg)

        if response.status_code == 202:
            return func.HttpResponse("Email sent successfully.")
        else:
            return func.HttpResponse(f"Failed to send email: {response.status_code} - {response.text}", status_code=500)

    except Exception as e:
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)


# COMMAND ----------

# MAGIC %pip install msal requests

# COMMAND ----------

# ERROR: Could not find a version that satisfies the requirement msalimport (from versions: none)
# ERROR: No matching distribution found for msalimport
dbutils.library.restartPython()

# COMMAND ----------

import requests

from msal import ConfidentialClientApplication

def send_email(recipient, subject, body):
    tenant_id = "60419507-249c-412c-bf2e-a187855acd2f"
    client_id = "15e6792e-c481-462a-bc6b-c402732dcc39"
    client_secret = "Qbp8Q~Y9~h18-dGi5iz38hgeRwBS1oxfWAXn2c0s"
    sender_email = "saipraneeth.nan@gmail.com"

    authority = f"https://login.microsoftonline.com/{tenant_id}"
    scope = ["https://graph.microsoft.com/.default"]

    app = ConfidentialClientApplication(client_id, authority=authority, client_credential=client_secret)
    token = app.acquire_token_for_client(scopes=scope)

    if "access_token" not in token:
        raise Exception(f"Authentication failed: {token.get('error_description')}")

    headers = {
        "Authorization": f"Bearer {token['access_token']}",
        "Content-Type": "application/json"
    }

    email_msg = {
        "message": {
            "subject": subject,
            "body": {
                "contentType": "Text",
                "content": body
            },
            "toRecipients": [
                {
                    "emailAddress": {
                        "address": recipient
                    }
                }
            ]
        }
    }

    send_url = f"https://graph.microsoft.com/v1.0/users/{sender_email}/sendMail"
    response = requests.post(send_url, headers=headers, json=email_msg)

    if response.status_code == 202:
        print("Email sent successfully.")
    else:
        raise Exception(f"Failed to send email: {response.status_code} - {response.text}")

# Example usage:
send_email("saipraneeth.com", "Test Subject", "Hello from Databricks!")


# COMMAND ----------

import logging
import requests
import azure.functions as func
from msal import ConfidentialClientApplication

def main(req: func.HttpRequest) -> func.HttpResponse:
    recipient = req.params.get('to')
    subject = req.params.get('subject')
    body = req.params.get('body')

    # Azure AD App credentials
    client_id = "YOUR_CLIENT_ID"
    client_secret = "YOUR_CLIENT_SECRET"
    tenant_id = "YOUR_TENANT_ID"
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    scope = ["https://graph.microsoft.com/.default"]

    # Authenticate with MSAL
    app = ConfidentialClientApplication(
        client_id, authority=authority, client_credential=client_secret
    )
    token = app.acquire_token_for_client(scopes=scope)

    if "access_token" not in token:
        return func.HttpResponse("Auth failed", status_code=401)

    headers = {
        'Authorization': 'Bearer ' + token['access_token'],
        'Content-Type': 'application/json'
    }

    email_msg = {
        "message": {
            "subject": subject,
            "body": {
                "contentType": "Text",
                "content": body
            },
            "toRecipients": [
                {
                    "emailAddress": {
                        "address": recipient
                    }
                }
            ]
        }
    }

    response = requests.post(
        'https://graph.microsoft.com/v1.0/users/YOUR_EMAIL_ADDRESS/sendMail',
        headers=headers,
        json=email_msg
    )

    if response.status_code == 202:
        return func.HttpResponse("Email sent successfully.")
    else:
        return func.HttpResponse(f"Failed: {response.text}", status_code=500)


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import TimestampType
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
import os

# Initialize Spark with Delta support
builder = SparkSession.builder \
    .appName("SCD2_Delta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Paths
scd2_path = "/mnt/datalake/scd2_customer_dim"

# Sample incoming data
incoming_data = [
    (1,'manish','bangalore','india','2023‑03‑11'),
    (5,'ayush','mosco','russia','2023‑09‑07'),
    (6,'rajat','jaipur','india','2023‑08‑10')
]
incoming_schema = ['id','name','city','country','sales_date']
incoming_df = spark.createDataFrame(incoming_data, incoming_schema) \
    .withColumn("effective_start_date", col("sales_date").cast(TimestampType())) \
    .withColumn("effective_end_date", lit(None).cast(TimestampType())) \
    .withColumn("active", lit("Y"))

# Initial dimension seed if not exists
if not DeltaTable.isDeltaTable(spark, scd2_path):
    base_data = [
        (1,'manish','gurgaon','india','Y','2022‑09‑25',None),
        (2,'vikash','patna','india','Y','2023‑08‑12',None),
        (3,'nikita','delhi','india','Y','2023‑09‑10',None),
        (4,'rakesh','jaipur','india','Y','2023‑06‑10',None),
        (5,'ayush','NY','USA','Y','2023‑06‑10',None)
    ]
    base_schema = ['id','name','city','country','active','effective_start_date','effective_end_date']
    spark.createDataFrame(base_data, base_schema) \
         .write.format("delta").mode("overwrite").save(scd2_path)

# Perform SCD2 merge
dim_table = DeltaTable.forPath(spark, scd2_path)

dim_table.alias("dim") \
  .merge(
    incoming_df.alias("up"),
    "dim.id = up.id AND dim.active = 'Y'"
  ) \
  .whenMatchedUpdate(
    condition="dim.city <> up.city OR dim.country <> up.country",
    set={
      "active": lit("N"),
      "effective_end_date": col("up.effective_start_date")
    }
  ) \
  .whenNotMatchedInsert(
    values={
      "id": col("up.id"),
      "name": col("up.name"),
      "city": col("up.city"),
      "country": col("up.country"),
      "active": lit("Y"),
      "effective_start_date": col("up.effective_start_date"),
      "effective_end_date": lit(None)
    }
  ) \
  .execute()

# Display final dimension
spark.read.format("delta").load(scd2_path) \
    .orderBy("id", "effective_start_date") \
    .show(truncate=False)


# COMMAND ----------

# MAGIC %pip install azure-storage-blob azure-identity

# COMMAND ----------

from azure.storage.blob import generate_blob_sas, BlobServiceClient
from datetime import datetime, timedelta

# Define your storage account details
account_url = "https://stallion2192.blob.core.windows.net"
account_key = "qnOUN3DoH5j3mb3dtUCPsI4KQedzzfbl+GtiJh1lh6ai+9AAoRcyzkWz1XbjoBlTl9rjVwuTm65L+AStYGr1LA=="  # Replace with your actual account key
container_name = "jerry"
blob_name = "things/uploads/dummy_src_1234.csv"

# Initialize the BlobServiceClient
blob_service_client = BlobServiceClient(account_url=account_url, credential=account_key)

# Generate a SAS token for the source blob with read permission
sas_token = generate_blob_sas(
    account_name=blob_service_client.account_name,
    container_name=container_name,
    blob_name=blob_name,
    account_key=account_key,
    permission="r",  # Read permission
    expiry=datetime.utcnow() + timedelta(hours=1)  # Token valid for 1 hour
)

# Construct the source blob URL with the SAS token
source_blob_url = f"{account_url}/{container_name}/{blob_name}?{sas_token}"

# Define the destination blob path
destination_blob_path = "enrollment/dummy_src_1234.csv"

# Initialize the BlobClient for the destination blob
destination_blob_client = blob_service_client.get_blob_client(container=container_name, blob=destination_blob_path)

# Start the copy operation
destination_blob_client.start_copy_from_url(source_blob_url)

print(f"Copy operation started for {source_blob_url} to {destination_blob_path}")


# COMMAND ----------

from azure.storage.blob import BlobServiceClient

conn_str = "DefaultEndpointsProtocol=https;AccountName=stallion2192;AccountKey=qnOUN3DoH5j3mb3dtUCPsI4KQedzzfbl+GtiJh1lh6ai+9AAoRcyzkWz1XbjoBlTl9rjVwuTm65L+AStYGr1LA==;EndpointSuffix=core.windows.net"

container_name = "jerry"
folderpath = "thing/uploads/"
filename = "dummy_src_1234.csv"

blob_service_client = BlobServiceClient.from_connection_string(conn_str)
container_client = blob_service_client.get_container_client(container_name)
blob_name = f"{folderpath}{filename}"

def file_exists(container_client, blob_name):
    try:
        container_client.get_blob_client(blob_name).get_blob_properties()
        return True
    except:
        return False

exists = file_exists(container_client, blob_name)
print(f"File exists: {exists}")


# COMMAND ----------

import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from datetime import datetime, timezone

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing HTTP request.')

    # Extract parameters from the query string or request body
    folderpath = req.params.get('folderpath')
    filename = req.params.get('filename')
    if not folderpath or not filename:
        return func.HttpResponse(
            "Please pass 'folderpath' and 'filename' in the query string",
            status_code=400
        )

    # Initialize BlobServiceClient
    conn_str = "DefaultEndpointsProtocol=https;AccountName=stallion2192;AccountKey=qnOUN3DoH5j3mb3dtUCPsI4KQedzzfbl+GtiJh1lh6ai+9AAoRcyzkWz1XbjoBlTl9rjVwuTm65L+AStYGr1LA==;EndpointSuffix=core.windows.net"
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    container_name = "jerry"  # Adjust as necessary
    container_client = blob_service_client.get_container_client(container_name)

    # Define blob paths
    path_blob = f"{folderpath}{filename}"
    temp_blob = f"{folderpath}temp/{filename}"
    processed_blob = f"{folderpath}processed/{filename}"
    enrollment_blob = f"enrollment/{filename}"

    # Helper functions
    def blob_exists(blob_name):
        try:
            container_client.get_blob_client(blob_name).get_blob_properties()
            return True
        except:
            return False

    def copy_blob(src_blob_name, dest_blob_name):
        src_blob_url = container_client.get_blob_client(src_blob_name).url
        dest_blob_client = container_client.get_blob_client(dest_blob_name)
        dest_blob_client.start_copy_from_url(src_blob_url)

    def move_blob(src_blob_name, dest_blob_name):
        copy_blob(src_blob_name, dest_blob_name)
        container_client.delete_blob(src_blob_name)

    def delete_blob(blob_name):
        container_client.delete_blob(blob_name)

    # Processing logic
    current_datetime = datetime.now(timezone.utc)
    if folderpath == "thing/uploads/":
        if blob_exists(path_blob):
            copy_blob(path_blob, enrollment_blob)
            move_blob(path_blob, temp_blob)

    elif folderpath == "enrollment/":
        if blob_exists(temp_blob):
            move_blob(temp_blob, processed_blob)
            delete_blob(temp_blob)
            return func.HttpResponse("Processed successfully", status_code=200)

    elif folderpath == "":
        if not blob_exists(temp_blob) and not blob_exists(processed_blob):
            if current_datetime.day > 7:
                return func.HttpResponse("No files to process", status_code=200)
        if blob_exists(temp_blob) and not blob_exists(processed_blob):
            # Check last modified time
            return func.HttpResponse("Processing in progress", status_code=200)

    return func.HttpResponse("No action taken", status_code=200)


# COMMAND ----------

#file first oka folder vasthundi
things/uploads/file --> tmp
        -->processed
        -->enrollment
second step:
    enrollment/file --> processed(with next 7 days)



# COMMAND ----------

# DBTITLE 1,working scenario
from azure.storage.blob import BlobServiceClient

conn_str = "DefaultEndpointsProtocol=https;AccountName=stallion2192;AccountKey=qnOUN3DoH5j3mb3dtUCPsI4KQedzzfbl+GtiJh1lh6ai+9AAoRcyzkWz1XbjoBlTl9rjVwuTm65L+AStYGr1LA==;EndpointSuffix=core.windows.net"

container_name = "jerry"
folderpath = "thing/uploads/"
filename = "dummy_src_1234.csv"

blob_service_client = BlobServiceClient.from_connection_string(conn_str)
container_client = blob_service_client.get_container_client(container_name)
blob_name = f"{folderpath}{filename}"

def file_exists(container_client, blob_name):
    try:
        container_client.get_blob_client(blob_name).get_blob_properties()
        return True
    except:
        return False

exists = file_exists(container_client, blob_name)
print(f"File exists: {exists}")
from datetime import datetime, timezone
from azure.storage.blob import BlobServiceClient
import os

# Parameters (simulate widgets)

folderpath = "thing/uploads/"
filename = "dummy_src_1234.csv"
def list_blobs_with_prefix(container_client, prefix):
    return [blob.name for blob in container_client.list_blobs(name_starts_with=prefix)]

def blob_exists(container_client, blob_name):
    try:
        container_client.get_blob_client(blob_name).get_blob_properties()
        return True
    except:
        return False

def get_blob_last_modified(container_client, blob_name):
    props = container_client.get_blob_client(blob_name).get_blob_properties()
    return props.last_modified

def copy_blob(container_client, src_blob_name, dest_blob_name):
    src_blob_url = container_client.get_blob_client(src_blob_name).url
    dest_blob_client = container_client.get_blob_client(dest_blob_name)
    dest_blob_client.start_copy_from_url(src_blob_url)

def move_blob(container_client, src_blob_name, dest_blob_name):
    copy_blob(container_client, src_blob_name, dest_blob_name)
    container_client.delete_blob(src_blob_name)

def delete_blob(container_client, blob_name):
    container_client.delete_blob(blob_name)

current_datetime = datetime.now(timezone.utc)

uploads_prefix = "thing/uploads/"
temp_prefix = f"{uploads_prefix}temp/"
processed_prefix = f"{uploads_prefix}processed/"
enrollment_prefix = "enrollment/"

path_blob = f"{uploads_prefix}{filename}"
temp_blob = f"{temp_prefix}{filename}"
processed_blob = f"{processed_prefix}{filename}"
enrollment_blob = f"{enrollment_prefix}{filename}"

processed_files = list_blobs_with_prefix(container_client, processed_prefix)
temp_files = list_blobs_with_prefix(container_client, temp_prefix)

if folderpath == "thing/uploads/":  # match your variable
    if blob_exists(container_client, path_blob):
        copy_blob(container_client, path_blob, enrollment_blob)
        move_blob(container_client, path_blob, temp_blob)

elif folderpath == "enrollment/":
    if temp_files:
        move_blob(container_client, temp_blob, processed_blob)
        delete_blob(container_client, temp_blob)
        dbutils.notebook.exit('3')

elif folderpath == "":
    if not temp_files and not processed_files:
        if current_datetime.day > 7:
            dbutils.notebook.exit('1')
    if temp_files and not processed_files:
        last_modified_time = get_blob_last_modified(container_client, temp_blob)
        if (last_modified_time.day + 7) > current_datetime.day:
            dbutils.notebook.exit('2')
    elif processed_files:
        dbutils.notebook.exit('already processed')


# COMMAND ----------

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from datetime import datetime, timezone
import pandas as pd
from io import BytesIO

# Azure storage account connection string
conn_str = "DefaultEndpointsProtocol=https;AccountName=stallion2192;AccountKey=qnOUN3DoH5j3mb3dtUCPsI4KQedzzfbl+GtiJh1lh6ai+9AAoRcyzkWz1XbjoBlTl9rjVwuTm65L+AStYGr1LA==;EndpointSuffix=core.windows.net"

# Containers
source_container_name = "jerry"  # your source container
process_container_name = "des" # same as source per your code?

# Inputs (simulate widgets)
#folderpath = "thing/uploads/"  # "uploads/", "enrollment/", or ""
folderpath ="enrollment/"
filename = "dummy_src_1234.csv"

blob_service_client = BlobServiceClient.from_connection_string(conn_str)
source_container_client = blob_service_client.get_container_client(source_container_name)
process_container_client = blob_service_client.get_container_client(process_container_name)

# Blob paths (relative inside containers)
path_blob = f"thing/uploads/{filename}"
temp_blob = f"thing/uploads/temp/{filename}"
processed_blob = f"uploads/processed/{filename}"
enrollment_blob = f"enrollment/{filename}"

def list_blobs_with_prefix(container_client, prefix):
    """Return list of blob names starting with prefix."""
    blob_names = []
    blobs = container_client.list_blobs(name_starts_with=prefix)
    for blob in blobs:
        blob_names.append(blob.name)
    return blob_names

def blob_exists(container_client, blob_name):
    """Check if blob exists."""
    try:
        container_client.get_blob_client(blob_name).get_blob_properties()
        return True
    except:
        return False

def get_blob_last_modified(container_client, blob_name):
    """Get last modified datetime of a blob in UTC."""
    props = container_client.get_blob_client(blob_name).get_blob_properties()
    return props.last_modified  # datetime with timezone info

def copy_blob(src_container_client, src_blob_name, dest_container_client, dest_blob_name):
    """Copy blob from source container to destination container."""
    src_blob_url = src_container_client.get_blob_client(src_blob_name).url
    dest_blob_client = dest_container_client.get_blob_client(dest_blob_name)
    copy = dest_blob_client.start_copy_from_url(src_blob_url)
    # Optionally wait until copy done, or poll copy status if needed

def delete_blob(container_client, blob_name):
    """Delete a blob."""
    container_client.delete_blob(blob_name)

def move_blob(src_container_client, src_blob_name, dest_container_client, dest_blob_name):
    """Move blob by copying and deleting source."""
    copy_blob(src_container_client, src_blob_name, dest_container_client, dest_blob_name)
    delete_blob(src_container_client, src_blob_name)

def display_processed_file(container_client, blob_name):
    blob_client = container_client.get_blob_client(blob_name)
    stream = blob_client.download_blob()
    data = stream.readall()
    df = pd.read_csv(BytesIO(data))
    display(df)

current_datetime = datetime.now(timezone.utc)
print(f"Checking folderpath: {folderpath}")
if folderpath == "things/uploads/":
    if blob_exists(source_container_client, path_blob):
        print(f"Blob {path_blob} exists.")
        # copy to enrollment
        copy_blob(source_container_client, path_blob, process_container_client, enrollment_blob)
        # move to temp
        move_blob(source_container_client, path_blob, source_container_client, temp_blob)

elif folderpath == "enrollment/":
    temp_files = list_blobs_with_prefix(source_container_client, "uploads/temp/")
    processed_files = list_blobs_with_prefix(source_container_client, "uploads/processed/")

    if len(temp_files) != 0 and len(processed_files) != 0:
        # Move all temp files to processed folder
        for blob_name in temp_files:
            # Extract filename from blob_name
            fname = blob_name.split('/')[-1]
            dest_blob_name = f"uploads/processed/{fname}"
            move_blob(source_container_client, blob_name, source_container_client, dest_blob_name)
        # No directory deletion needed in blob storage
        print('3')
        # Display the processed file
        if blob_exists(source_container_client, processed_blob):
            display_processed_file(source_container_client, processed_blob)
        exit(0)

elif folderpath == "":
    temp_files = list_blobs_with_prefix(source_container_client, "uploads/temp/")
    processed_files = list_blobs_with_prefix(source_container_client, "uploads/processed/")

    if len(temp_files) == 0 and len(processed_files) == 0:
        if current_datetime.day > 7:
            print('1')
            exit(0)

    if len(temp_files) > 0 and len(processed_files) == 0:
        # get last modified of first temp file
        first_temp_blob_name = temp_files[0]
        last_modified_time = get_blob_last_modified(source_container_client, first_temp_blob_name)
        if (last_modified_time.day + 7) > current_datetime.day:
            print('2')
            exit(0)

    if len(processed_files) != 0:
        print('already processed')
        # Display the processed file
        if blob_exists(source_container_client, processed_blob):
            display_processed_file(source_container_client, processed_blob)
        exit(0)
#

# COMMAND ----------

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from datetime import datetime, timezone

# Azure storage account connection string
conn_str = "DefaultEndpointsProtocol=https;AccountName=stallion2192;AccountKey=qnOUN3DoH5j3mb3dtUCPsI4KQedzzfbl+GtiJh1lh6ai+9AAoRcyzkWz1XbjoBlTl9rjVwuTm65L+AStYGr1LA==;EndpointSuffix=core.windows.net"

# Containers
source_container_name = "jerry"  # your source container
process_container_name = "des" # same as source per your code?

# Inputs (simulate widgets)
#folderpath = "thing/uploads/"  # "uploads/", "enrollment/", or ""
folderpath="enrollment/"
filename = "dummy_src_1234.csv"

blob_service_client = BlobServiceClient.from_connection_string(conn_str)
source_container_client = blob_service_client.get_container_client(source_container_name)
process_container_client = blob_service_client.get_container_client(process_container_name)

# Blob paths (relative inside containers)
path_blob = f"thing/uploads/{filename}"
temp_blob = f"thing/uploads/temp/{filename}"
processed_blob = f"uploads/processed/{filename}"
enrollment_blob = f"enrollment/{filename}"

def list_blobs_with_prefix(container_client, prefix):
    """Return list of blob names starting with prefix."""
    blob_names = []
    blobs = container_client.list_blobs(name_starts_with=prefix)
    for blob in blobs:
        blob_names.append(blob.name)
    return blob_names

def blob_exists(container_client, blob_name):
    """Check if blob exists."""
    try:
        container_client.get_blob_client(blob_name).get_blob_properties()
        return True
    except:
        return False

def get_blob_last_modified(container_client, blob_name):
    """Get last modified datetime of a blob in UTC."""
    props = container_client.get_blob_client(blob_name).get_blob_properties()
    return props.last_modified  # datetime with timezone info

def copy_blob(src_container_client, src_blob_name, dest_container_client, dest_blob_name):
    """Copy blob from source container to destination container."""
    src_blob_url = src_container_client.get_blob_client(src_blob_name).url
    dest_blob_client = dest_container_client.get_blob_client(dest_blob_name)
    copy = dest_blob_client.start_copy_from_url(src_blob_url)
    # Optionally wait until copy done, or poll copy status if needed

def delete_blob(container_client, blob_name):
    """Delete a blob."""
    container_client.delete_blob(blob_name)

def move_blob(src_container_client, src_blob_name, dest_container_client, dest_blob_name):
    """Move blob by copying and deleting source."""
    copy_blob(src_container_client, src_blob_name, dest_container_client, dest_blob_name)
    delete_blob(src_container_client, src_blob_name)

current_datetime = datetime.now(timezone.utc)
print(f"Checking folderpath: {folderpath}")
if folderpath == "things/uploads/":
    if blob_exists(source_container_client, path_blob):
        print(f"Blob {path_blob} exists.")
        # copy to enrollment
        copy_blob(source_container_client, path_blob, process_container_client, enrollment_blob)
        # move to temp
        move_blob(source_container_client, path_blob, process_container_client, temp_blob)

elif folderpath == "enrollment/":
    temp_files = list_blobs_with_prefix(source_container_client, "uploads/temp/")
    processed_files = list_blobs_with_prefix(source_container_client, "uploads/processed/")

    if len(temp_files) != 0 and len(processed_files) != 0:
        # Move all temp files to processed folder
        for blob_name in temp_files:
            # Extract filename from blob_name
            fname = blob_name.split('/')[-1]
            dest_blob_name = f"uploads/processed/{fname}"
            move_blob(source_container_client, blob_name, process_container_client, dest_blob_name)
        # No directory deletion needed in blob storage
        print('3')
        exit(0)

elif folderpath == "":
    temp_files = list_blobs_with_prefix(source_container_client, "uploads/temp/")
    processed_files = list_blobs_with_prefix(source_container_client, "uploads/processed/")

    if len(temp_files) == 0 and len(processed_files) == 0:
        if current_datetime.day > 7:
            print('1')
            exit(0)

    if len(temp_files) > 0 and len(processed_files) == 0:
        # get last modified of first temp file
        first_temp_blob_name = temp_files[0]
        last_modified_time = get_blob_last_modified(source_container_client, first_temp_blob_name)
        if (last_modified_time.day + 7) > current_datetime.day:
            print('2')
            exit(0)

    if len(processed_files) != 0:
        print('already processed')
        exit(0)
#

# COMMAND ----------

# from datetime import datetime
# from azure.storage.blob import BlobServiceClient

# # Credentials—hard coded (for testing only!)
# account_url = "https://stallion2192.blob.core.windows.net"
# account_key = "qnOUN3DoH5j3mb3dtUCPsI4KQedzzfbl+GtiJh1lh6ai+9AAoRcyzkWz1XbjoBlTl9rjVwuTm65L+AStYGr1LA=="  # retrieve from secrets in production

# blob_service = BlobServiceClient(account_url=account_url, credential=account_key)
# src = blob_service.get_container_client("jerry")

# folderpath = "things/uploads/"   # test input or widget param
# filename = "dummy_src_1234.csv"

# # helper functions
# def list_blobs(prefix):
#     return list(src.list_blobs(name_starts_with=prefix))

# def copy_blob(src_name, dest_name):
#     src_blob = src.get_blob_client(src_name)
#     dest_blob = src.get_blob_client(dest_name)
#     dest_blob.start_copy_from_url(src_blob.url)

# def delete_blob(name):
#     src.delete_blob(name)

# # Define paths
# path = f"{folderpath}{filename}"
# temp_prefix = f"{folderpath}temp/"
# processed_prefix = f"{folderpath}processed/"
# enrollment_prefix = "enrollment/"

# processed_blobs = list_blobs(processed_prefix + filename)
# temp_blobs = list_blobs(temp_prefix + filename)

# #now = datetime.utcnow()

# now = datetime.now(timezone.utc)

# if folderpath == "things/uploads/":
#     if len(list_blobs(path)) == 1:
#         copy_blob(path, enrollment_prefix + filename)
#         copy_blob(path, temp_prefix + filename)
#         delete_blob(path)

# elif folderpath == "enrollment/":
#     if temp_blobs:
#         copy_blob(temp_prefix + filename, processed_prefix + filename)
#         delete_blob(temp_prefix + filename)
#         print("3")

# elif folderpath == "":
#     if not temp_blobs and not processed_blobs and now.day > 7:
#         print("1"); exit(0)
#     if temp_blobs and not processed_blobs:
#         props = src.get_blob_client(temp_blobs[0].name).get_blob_properties()
#         mod_time = props.last_modified
#         if (mod_time.day + 7) > now.day:
#             print("2"); exit(0)
#     elif processed_blobs:
#         print("already processed"); exit(0)


# COMMAND ----------

import os
from datetime import datetime
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient

# Azure credentials and storage info
account_url = os.getenv("https://stallion2192.blob.core.windows.net")  # e.g. "https://myaccount.blob.core.windows.net"
credential = DefaultAzureCredential()
blob_service = BlobServiceClient(account_url="account_url", credential=credential)

# Input parameters (from Azure Function or HTTP API)
folderpath = os.getenv("FOLDERPATH", "")     # e.g. "uploads/"
filename = os.getenv("FILENAME", "")

# Containers and blob paths
src_container_name = "jerry"
src = blob_service.get_container_client(src_container_name)

def list_blobs(prefix: str):
    return list(src.list_blobs(name_starts_with=prefix))

def copy_blob(src_name: str, dest_name: str):
    src_blob = src.get_blob_client(src_name)
    dest_blob = src.get_blob_client(dest_name)
    dest_blob.start_copy_from_url(src_blob.url)

def delete_blob(name: str):
    src.delete_blob(name)

# Define paths
path = f"{folderpath}{filename}"
temp_prefix = f"{folderpath}temp/"
processed_prefix = f"{folderpath}processed/"
enrollment_prefix = f"enrollment/"

processed_blobs = list_blobs(processed_prefix + filename)
temp_blobs = list_blobs(temp_prefix + filename)

now = datetime.utcnow()

if folderpath == "uploads/":
    if len(list_blobs(path)) == 1:
        copy_blob(path, enrollment_prefix + filename)
        copy_blob(path, temp_prefix + filename)
        delete_blob(path)

elif folderpath == "enrollment/":
    if temp_blobs and temp_blobs:
        copy_blob(temp_prefix + filename, processed_prefix + filename)
        delete_blob(temp_prefix + filename)
        print("3")  # exit 3 equivalent

elif folderpath == "":
    if not temp_blobs and not processed_blobs:
        if now.day > 7:
            print("1")
            exit(0)
    if temp_blobs and not processed_blobs:
        props = src.get_blob_client(temp_blobs[0].name).get_blob_properties()
        mod_time = props.last_modified
        if (mod_time.day + 7) > now.day:
            print("2")
            exit(0)
    elif processed_blobs:
        print("already processed")
        exit(0)


# COMMAND ----------

import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from datetime import datetime, timezone

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('Processing HTTP request.')

        # Extract query parameters
        folderpath = req.params.get('folderpath')
        filename = req.params.get('filename')

        logging.info(f"Received folderpath: {folderpath}, filename: {filename}")

        if not folderpath or not filename:
            return func.HttpResponse(
                "Please pass 'folderpath' and 'filename' in the query string",
                status_code=400
            )

        # Initialize BlobServiceClient
        conn_str = "DefaultEndpointsProtocol=https;AccountName=stallion2192;AccountKey=qnOUN3DoH5j3mb3dtUCPsI4KQedzzfbl+GtiJh1lh6ai+9AAoRcyzkWz1XbjoBlTl9rjVwuTm65L+AStYGr1LA==;EndpointSuffix=core.windows.net"  # Secure this via environment variables or Azure Key Vault
        blob_service_client = BlobServiceClient.from_connection_string(conn_str)
        container_name = "jerry"
        container_client = blob_service_client.get_container_client(container_name)

        # Blob paths
        path_blob = f"{folderpath}{filename}"
        temp_blob = f"{folderpath}temp/{filename}"
        processed_blob = f"{folderpath}processed/{filename}"
        enrollment_blob = f"enrollment/{filename}"

        # Helper functions
        def blob_exists(blob_name):
            try:
                container_client.get_blob_client(blob_name).get_blob_properties()
                logging.info(f"Blob exists: {blob_name}")
                return True
            except Exception as e:
                logging.warning(f"Blob does not exist: {blob_name}. Reason: {str(e)}")
                return False

        def copy_blob(src_blob_name, dest_blob_name):
            try:
                src_blob_url = container_client.get_blob_client(src_blob_name).url
                dest_blob_client = container_client.get_blob_client(dest_blob_name)
                dest_blob_client.start_copy_from_url(src_blob_url)
                logging.info(f"Copied blob from {src_blob_name} to {dest_blob_name}")
            except Exception as e:
                logging.error(f"Failed to copy blob from {src_blob_name} to {dest_blob_name}: {str(e)}")

        def move_blob(src_blob_name, dest_blob_name):
            copy_blob(src_blob_name, dest_blob_name)
            try:
                container_client.delete_blob(src_blob_name)
                logging.info(f"Deleted original blob after copy: {src_blob_name}")
            except Exception as e:
                logging.error(f"Failed to delete blob {src_blob_name}: {str(e)}")

        def delete_blob(blob_name):
            try:
                container_client.delete_blob(blob_name)
                logging.info(f"Deleted blob: {blob_name}")
            except Exception as e:
                logging.error(f"Failed to delete blob {blob_name}: {str(e)}")

        # Processing logic
        current_datetime = datetime.now(timezone.utc)

        if folderpath == "thing/uploads/":
            if blob_exists(path_blob):
                copy_blob(path_blob, enrollment_blob)
                move_blob(path_blob, temp_blob)
                return func.HttpResponse("Upload processed", status_code=200)
            else:
                return func.HttpResponse("Upload blob does not exist", status_code=404)

        elif folderpath == "enrollment/":
            if blob_exists(temp_blob):
                move_blob(temp_blob, processed_blob)
                delete_blob(temp_blob)
                return func.HttpResponse("Enrollment processed", status_code=200)
            else:
                return func.HttpResponse("Enrollment blob not found", status_code=404)

        elif folderpath == "":
            if not blob_exists(temp_blob) and not blob_exists(processed_blob):
                if current_datetime.day > 7:
                    return func.HttpResponse("No files to process", status_code=200)
            if blob_exists(temp_blob) and not blob_exists(processed_blob):
                # More detailed logic can be added here
                return func.HttpResponse("Processing in progress", status_code=200)

        return func.HttpResponse("No action taken", status_code=200)

    except Exception as e:
        logging.exception("Unhandled error occurred")
        return func.HttpResponse(f"Internal Server Error: {str(e)}", status_code=500)

# COMMAND ----------

# Databricks notebook or script: Azure Blob File Management

import logging
from azure.storage.blob import BlobServiceClient
from azure.identity import ManagedIdentityCredential
from datetime import datetime, timezone
import os

# Setup logging
logging.basicConfig(level=logging.INFO)

# Define input widgets (Databricks UI)
dbutils.widgets.text("folderpath", "thing/uploads/")
dbutils.widgets.text("filename", "dummy_src_1234.csv")

folderpath = dbutils.widgets.get("folderpath")
filename = dbutils.widgets.get("filename")

logging.info(f"Received folderpath: {folderpath}, filename: {filename}")

# Configs (Use DB secrets or environment variables if needed)
azure_storage_account_name_src = os.getenv("DefaultEndpointsProtocol=https;AccountName=stallion2192;AccountKey=qnOUN3DoH5j3mb3dtUCPsI4KQedzzfbl+GtiJh1lh6ai+9AAoRcyzkWz1XbjoBlTl9rjVwuTm65L+AStYGr1LA==;EndpointSuffix=core.windows.net")  # e.g., storazdbkmldev
azure_storage_account_name_proc = os.getenv("DefaultEndpointsProtocol=https;AccountName=twitter2192;AccountKey=BvuXZo6pE3xIRMnhPwwyjeV7KcUW6gUc/zcyZxdBXYYNjBq798iV3iJjbq2a4VK7xv7J4yYxkigJ+AStWBy3/w==;EndpointSuffix=core.windows.net")  # e.g., stodemldev

blob_url_src = f"https://stallion2192.blob.core.windows.net"
blob_url_proc = f"https://twitter2192.blob.core.windows.net"

container_src = 'src'
container_proc = 'tgt'

# Authenticate and create blob clients
credential = ManagedIdentityCredential()
blob_service_client_src = BlobServiceClient(account_url=blob_url_src, credential=credential)
blob_service_client_proc = BlobServiceClient(account_url=blob_url_proc, credential=credential)
container_client_src = blob_service_client_src.get_container_client(container_src)
container_client_proc = blob_service_client_proc.get_container_client(container_proc)

current_datetime = datetime.now(timezone.utc)

# --- Helper Functions ---

def blob_exists(blob_name, container_client):
    try:
        container_client.get_blob_client(blob_name).get_blob_properties()
        logging.info(f"Blob exists: {blob_name}")
        return True
    except Exception as e:
        logging.warning(f"Blob does not exist: {blob_name}. Reason: {str(e)}")
        return False

def copy_blob(src_blob_name, dest_blob_name, src_container_client, dest_container_client):
    try:
        src_blob_url = src_container_client.get_blob_client(src_blob_name).url
        dest_blob_client = dest_container_client.get_blob_client(dest_blob_name)
        dest_blob_client.start_copy_from_url(src_blob_url)
        logging.info(f"Copied blob from {src_blob_name} to {dest_blob_name}")
    except Exception as e:
        logging.error(f"Failed to copy blob from {src_blob_name} to {dest_blob_name}: {str(e)}")

def move_blob(src_blob_name, dest_blob_name, src_container_client, dest_container_client):
    copy_blob(src_blob_name, dest_blob_name, src_container_client, dest_container_client)
    try:
        src_container_client.delete_blob(src_blob_name)
        logging.info(f"Deleted original blob after copy: {src_blob_name}")
    except Exception as e:
        logging.error(f"Failed to delete blob {src_blob_name}: {str(e)}")

def delete_blob(blob_name, container_client):
    try:
        container_client.delete_blob(blob_name)
        logging.info(f"Deleted blob: {blob_name}")
    except Exception as e:
        logging.error(f"Failed to delete blob {blob_name}: {str(e)}")

# --- Main Logic ---

try:
    path = f'uploads/{filename}'
    temp = f'uploads/temp/{filename}'
    processed = f'uploads/processed/{filename}'
    enrollment_path = f'deidentified/{filename}'

    processed_files = blob_exists(processed, container_client_src)
    temp_files = blob_exists(temp, container_client_src)

    blob_client = container_client_src.get_blob_client(temp)
    properties = blob_client.get_blob_properties()
    modification_datetime = properties.last_modified

    result = "No action taken"

    if folderpath == 'uploads/':
        if blob_exists(path, container_client_src):
            copy_blob(path, enrollment_path, container_client_src, container_client_proc)
            move_blob(path, temp, container_client_src, container_client_src)
            result = "upload processed"

    elif folderpath == 'outbound/':
        if temp_files:
            move_blob(temp, processed, container_client_src, container_client_src)
            delete_blob(temp, container_client_src)
            result = "3"

    elif folderpath == '':
        if not temp_files and not processed_files:
            if current_datetime.day > 7:
                result = "1"

        if temp_files and not processed_files:
            if (modification_datetime.day + 7) > current_datetime.day:
                result = "2"

        elif processed_files:
            result = "already processed"

    print(result)

except Exception as e:
    logging.exception("Unhandled error occurred")
    raise


# COMMAND ----------

import logging, time
from datetime import datetime, timezone, timedelta

from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# COMMAND ----------



# ─── 1. Hardcoded connection strings (replace with your own) ───────────────────
#
# These must include AccountName, AccountKey, EndpointSuffix=core.windows.net
#
CONN_STR_SRC = (
    "DefaultEndpointsProtocol=https;AccountName=stallion2192;"
    "AccountKey=qnOUN3DoH5j3mb3dtUCPsI4KQedzzfbl+GtiJh1lh6ai+9AAoRcyzkWz1XbjoBlTl9rjVwuTm65L+AStYGr1LA==;EndpointSuffix=core.windows.net"
)

CONN_STR_TGT = (
    "DefaultEndpointsProtocol=https;AccountName=twitter2192;"
    "AccountKey=BvuXZo6pE3xIRMnhPwwyjeV7KcUW6gUc/zcyZxdBXYYNjBq798iV3iJjbq2a4VK7xv7J4yYxkigJ+AStWBy3/w==;EndpointSuffix=core.windows.net"
)

# ─── 2. Blob service initialization (from connection string) ────────────────
svc_src = BlobServiceClient.from_connection_string(CONN_STR_SRC)
svc_tgt = BlobServiceClient.from_connection_string(CONN_STR_TGT)

container_src = svc_src.get_container_client("src")
container_tgt = svc_tgt.get_container_client("tgt")


# COMMAND ----------


# ─── 3. Input variables (hardcoded or replace with dbutils.widgets...) ═══════
folderpath = "uploads/"  # includes subfolder path
filename = "dummy_31_07_2025_18:09:10.csv"

fp = folderpath.rstrip("/") + "/"
path0 = fp + filename
temp0 = fp + "temp/" + filename
proc0 = fp + "processed/" + filename
deid0 = "deidentified/" + filename

logging.info(f"Paths ↓\n- main:  {path0}\n- temp:  {temp0}\n- proc:  {proc0}\n- deid:  {deid0}")

now = datetime.now(timezone.utc)
result = "no action"

# COMMAND ----------

# ─── 4. Utility functions ─────────────────────────────────────────────────────
def exists(container, blob_name):
    try:
        container.get_blob_client(blob_name).get_blob_properties()
        return True
    except ResourceNotFoundError:
        return False
    except Exception as err:
        logging.warning(f"exists check failed for '{blob_name}': {err}")
        return False

def wait_for_copy(dest_blob, timeout_sec=300, poll_interval=5):
    props = dest_blob.get_blob_properties()
    status = getattr(props.copy, "status", None)
    elapsed = 0
    while status and status.lower() == "pending":
        if elapsed >= timeout_sec:
            raise TimeoutError(f"Copy timed out after {timeout_sec}s")
        time.sleep(poll_interval)
        elapsed += poll_interval
        props = dest_blob.get_blob_properties()
        status = getattr(props.copy, "status", None)
    return (status or "").lower(), props

def copy_blob(src_name, dst_name):
    src_url = container_src.get_blob_client(src_name).url
    dest = container_tgt.get_blob_client(dst_name)
    logging.info(f"Starting copy: '{src_name}' → '{dst_name}'")
    dest.start_copy_from_url(src_url)
    status, props = wait_for_copy(dest)
    if status != "success":
        raise RuntimeError(f"Copy failed ({status})")
    logging.info(f"Copied successfully: {src_name} → {dst_name} (id: {props.copy.id})")
    return props

def move_blob(src_name, dst_name):
    props = copy_blob(src_name, dst_name)
    logging.info(f"Deleting source blob '{src_name}'")
    container_src.get_blob_client(src_name).delete_blob()
    return props


# COMMAND ----------

exists(container_src,folderpath+filename)

# COMMAND ----------


# ─── 5. File flow logic ───────────────────────────────────────────────────────
has_main = exists(container_src, path0)
has_temp = exists(container_src, temp0)
has_proc = exists(container_src, proc0)

logging.info(f"exists: main={has_main}, temp={has_temp}, processed={has_proc}")

if fp.endswith("uploads/") and has_main:
    move_blob(path0, temp0)
    copy_blob(temp0, deid0)
    result = "upload processed"

elif fp.endswith("outbound/") and has_temp:
    move_blob(temp0, proc0)
    result = "processed"

else:
    if not has_temp and not has_proc and now.day > 7:
        result = "1"
    elif has_temp and not has_proc:
        props = container_src.get_blob_client(temp0).get_blob_properties()
        if now - props.last_modified <= timedelta(days=7):
            result = "2"
    elif has_proc:
        result = "already processed"

print("RESULT →", result)


# COMMAND ----------

import logging
from azure.identity import ManagedIdentityCredential
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError

from datetime import datetime, timezone, timedelta
import os

logging.basicConfig(level=logging.INFO)
dbutils.widgets.text("folderpath", "uploads/")
dbutils.widgets.text("filename", "dummy_src_1234.csv")
folderpath = dbutils.widgets.get("folderpath")
filename = dbutils.widgets.get("filename")
logging.info(f"folderpath={folderpath!r}, filename={filename!r}")

# Use connection string or account name from env
conn_str_src = os.getenv("DefaultEndpointsProtocol=https;AccountName=stallion2192;AccountKey=qnOUN3DoH5j3mb3dtUCPsI4KQedzzfbl+GtiJh1lh6ai+9AAoRcyzkWz1XbjoBlTl9rjVwuTm65L+AStYGr1LA==;EndpointSuffix=core.windows.net")
conn_str_proc = os.getenv("DefaultEndpointsProtocol=https;AccountName=twitter2192;AccountKey=BvuXZo6pE3xIRMnhPwwyjeV7KcUW6gUc/zcyZxdBXYYNjBq798iV3iJjbq2a4VK7xv7J4yYxkigJ+AStWBy3/w==;EndpointSuffix=core.windows.net")
# azure_storage_account_name_src = os.getenv("DefaultEndpointsProtocol=https;AccountName=stallion2192;AccountKey=qnOUN3DoH5j3mb3dtUCPsI4KQedzzfbl+GtiJh1lh6ai+9AAoRcyzkWz1XbjoBlTl9rjVwuTm65L+AStYGr1LA==;EndpointSuffix=core.windows.net")  # e.g., storazdbkmldev
# azure_storage_account_name_proc = os.getenv("DefaultEndpointsProtocol=https;AccountName=twitter2192;AccountKey=BvuXZo6pE3xIRMnhPwwyjeV7KcUW6gUc/zcyZxdBXYYNjBq798iV3iJjbq2a4VK7xv7J4yYxkigJ+AStWBy3/w==;EndpointSuffix=core.windows.net")  # e.g., stodemldev
account_src = os.getenv("stallion2192")  # only used if conn_str not set
account_proc = os.getenv("twitter2192")

if conn_str_src:
    svc_src = BlobServiceClient.from_connection_string(conn_str_src)
else:
    if not account_src:
        raise ValueError("Provide either AZURE_STORAGE_SRC_CONN_STR or AZURE_STORAGE_ACCOUNT_SRC")
    svc_src = BlobServiceClient(f"https://{account_src}.blob.core.windows.net", credential=ManagedIdentityCredential())

if conn_str_proc:
    svc_proc = BlobServiceClient.from_connection_string(conn_str_proc)
else:
    if not account_proc:
        raise ValueError("Provide either AZURE_STORAGE_PROC_CONN_STR or AZURE_STORAGE_ACCOUNT_PROC")
    svc_proc = BlobServiceClient(f"https://{account_proc}.blob.core.windows.net", credential=ManagedIdentityCredential())

src = svc_src.get_container_client("src")
tgt = svc_proc.get_container_client("tgt")
now = datetime.now(timezone.utc)
result = "No action taken"


def exists(container_client, blob_name):
    try:
        container_client.get_blob_client(blob_name).get_blob_properties()
        return True
    except ResourceNotFoundError:
        return False
    except Exception as e:
        logging.warning(f"Error checking existence for {blob_name}: {e}")
        return False


def wait_for_copy(dest_blob, timeout_sec=300, poll_interval=5):
    elapsed = 0
    props = dest_blob.get_blob_properties()
    status = props.copy.status
    while status == "pending":
        if elapsed >= timeout_sec:
            raise TimeoutError(f"Copy from {props.copy.id} timed out after {timeout_sec}s")
        logging.info(f"Copy in progress: {elapsed}s elapsed")
        time.sleep(poll_interval)
        elapsed += poll_interval
        props = dest_blob.get_blob_properties()
        status = props.copy.status
    return status, props


def copy_blob(src_blob_name, dst_blob_name):
    src_url = src.get_blob_client(src_blob_name).url
    dest = src.get_blob_client(dst_blob_name)
    copy_result = dest.start_copy_from_url(src_url)
    status, props = wait_for_copy(dest)
    if status.lower() != "success":
        raise RuntimeError(f"Copy failed ({status}) for {src_blob_name} -> {dst_blob_name}")
    logging.info(f"Copy succeeded: {src_blob_name} -> {dst_blob_name}")
    return props


def move_blob(src_name, dst_name):
    props = copy_blob(src_name, dst_name)
    src.delete_blob(src_name)
    logging.info(f"Deleted source blob: {src_name} after copy")


# Clean up trailing slash
fp = folderpath.rstrip("/") + "/"
path = fp + filename
temp = fp + "temp/" + filename
processed = fp + "processed/" + filename
deidentified = "deidentified/" + filename

has_path = exists(src, path)
has_temp = exists(src, temp)
has_processed = exists(src, processed)

# Branch logic
if fp == "uploads/":
    if has_path:
        move_blob(path, temp)
        copy_blob(temp, deidentified)
        result = "upload processed"
elif fp == "outbound/":
    if has_temp:
        move_blob(temp, processed)
        result = "processed"
elif fp == "":
    if not has_temp and not has_processed and now.day > 7:
        result = "1"
    elif has_temp and not has_processed:
        props = src.get_blob_client(temp).get_blob_properties()
        if now - props.last_modified <= timedelta(days=7):
            result = "2"
    elif has_processed:
        result = "already processed"

print(result)


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

INFO:root:Received folderpath: thing/uploads/, filename: dummy_src_1234.csv
INFO:azure.identity._credentials.managed_identity:ManagedIdentityCredential will use IMDS
INFO:azure.core.pipeline.policies.http_logging_policy:Request URL: 'http://169.254.169.254/metadata/identity/oauth2/token?api-version=REDACTED&resource=REDACTED'
Request method: 'GET'
Request headers:
    'Metadata': 'REDACTED'
    'User-Agent': 'azsdk-python-identity/1.23.1 Python/3.12.3 (Linux-5.15.0-1091-azure-x86_64-with-glibc2.39)'
No body was attached to the request
INFO:azure.core.pipeline.policies.http_logging_policy:Request URL: 'http://169.254.169.254/metadata/identity/oauth2/token?api-version=REDACTED&resource=REDACTED'
Request method: 'GET'
Request headers:
    'Metadata': 'REDACTED'
    'User-Agent': 'azsdk-python-identity/1.23.1 Python/3.12.3 (Linux-5.15.0-1091-azure-x86_64-with-glibc2.39)'
No body was attached to the request
INFO:azure.core.pipeline.policies.http_logging_policy:Request URL: 'http://169.254.169.254/metadata/identity/oauth2/token?api-version=REDACTED&resource=REDACTED'
Request method: 'GET'
Request headers:
    'Metadata': 'REDACTED'
    'User-Agent': 'azsdk-python-identity/1.23.1 Python/3.12.3 (Linux-5.15.0-1091-azure-x86_64-with-glibc2.39)'
No body was attached to the request
INFO:azure.core.pipeline.policies.http_logging_policy:Request URL: 'http://169.254.169.254/metadata/identity/oauth2/token?api-version=REDACTED&resource=REDACTED'
Request method: 'GET'
Request headers:
    'Metadata': 'REDACTED'
    'User-Agent': 'azsdk-python-identity/1.23.1 Python/3.12.3 (Linux-5.15.0-1091-azure-x86_64-with-glibc2.39)'
No body was attached to the request
WARNING:azure.identity._internal.msal_managed_identity_client:ImdsCredential.get_token_info failed: ManagedIdentityCredential authentication unavailable, no response from the IMDS endpoint.
WARNING:azure.identity._internal.decorators:ManagedIdentityCredential.get_token_info failed: ManagedIdentityCredential authentication unavailable, no response from the IMDS endpoint.
INFO:azure.core.pipeline.policies.http_logging_policy:Request URL: 'http://169.254.169.254/metadata/identity/oauth2/token?api-version=REDACTED&resource=REDACTED'
Request method: 'GET'
Request headers:
    'Metadata': 'REDACTED'
    'User-Agent': 'azsdk-python-identity/1.23.1 Python/3.12.3 (Linux-5.15.0-1091-azure-x86_64-with-glibc2.39)'
No body was attached to the request
INFO:azure.core.pipeline.policies.http_logging_policy:Request URL: 'http://169.254.169.254/metadata/identity/oauth2/token?api-version=REDACTED&resource=REDACTED'
Request method: 'GET'
Request headers:
    'Metadata': 'REDACTED'
    'User-Agent': 'azsdk-python-identity/1.23.1 Python/3.12.3 (Linux-5.15.0-1091-azure-x86_64-with-glibc2.39)'
No body was attached to the request
INFO:azure.core.pipeline.policies.http_logging_policy:Request URL: 'http://169.254.169.254/metadata/identity/oauth2/token?api-version=REDACTED&resource=REDACTED'
Request method: 'GET'
Request headers:
    'Metadata': 'REDACTED'
    'User-Agent': 'azsdk-python-identity/1.23.1 Python/3.12.3 (Linux-5.15.0-1091-azure-x86_64-with-glibc2.39)'
No body was attached to the request
INFO:azure.core.pipeline.policies.http_logging_policy:Request URL: 'http://169.254.169.254/metadata/identity/oauth2/token?api-version=REDACTED&resource=REDACTED'
Request method: 'GET'
Request headers:
    'Metadata': 'REDACTED'
    'User-Agent': 'azsdk-python-identity/1.23.1 Python/3.12.3 (Linux-5.15.0-1091-azure-x86_64-with-glibc2.39)'
No body was attached to the request
WARNING:azure.identity._internal.msal_managed_identity_client:ImdsCredential.get_token_info failed: ManagedIdentityCredential authentication unavailable, no response from the IMDS endpoint.
WARNING:azure.identity._internal.decorators:ManagedIdentityCredential.get_token_info failed: ManagedIdentityCredential authentication unavailable, no response from the IMDS endpoint.

# COMMAND ----------

# MAGIC %pip install azure-functions

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# Azure Blob File Management Script
# Hardcoded values | Clean path handling | Fixed imports

import logging
import os
import time
from datetime import datetime, timezone, timedelta
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError

# ─── 1. Logging Setup ─────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# ─── 2. Hardcoded Inputs ──────────────────────────────────────────────────────
folderpath = "uploads/"  # or "outbound/"
filename = "dummy_src_1234.csv"

# ─── 3. Connection Strings ────────────────────────────────────────────────────
CONN_STR_SRC = (
    "DefaultEndpointsProtocol=https;AccountName=stallion2192;"
    "AccountKey=qnOUN3DoH5j3mb3dtUCPsI4KQedzzfbl+GtiJh1lh6ai+9AAoRcyzkWz1XbjoBlTl9rjVwuTm65L+AStYGr1LA==;"
    "EndpointSuffix=core.windows.net"
)

CONN_STR_TGT = (
    "DefaultEndpointsProtocol=https;AccountName=twitter2192;"
    "AccountKey=BvuXZo6pE3xIRMnhPwwyjeV7KcUW6gUc/zcyZxdBXYYNjBq798iV3iJjbq2a4VK7xv7J4yYxkigJ+AStWBy3/w==;"
    "EndpointSuffix=core.windows.net"
)

# ─── 4. Blob Clients ──────────────────────────────────────────────────────────
svc_src = BlobServiceClient.from_connection_string(CONN_STR_SRC)
svc_tgt = BlobServiceClient.from_connection_string(CONN_STR_TGT)

container_src = svc_src.get_container_client("src")
container_tgt = svc_tgt.get_container_client("tgt")

# ─── 5. Path Handling ─────────────────────────────────────────────────────────
def join_blob_path(*args):
    return "/".join(arg.strip("/") for arg in args if arg).strip("/")

path0 = join_blob_path(folderpath, filename)
temp0 = join_blob_path(folderpath, "temp", filename)
proc0 = join_blob_path(folderpath, "processed", filename)
deid0 = join_blob_path("deidentified", filename)

logging.info(f"Paths ↓\n- main:  {path0}\n- temp:  {temp0}\n- proc:  {proc0}\n- deid:  {deid0}")

# ─── 6. Utility Functions ─────────────────────────────────────────────────────
def exists(container, blob_name):
    try:
        container.get_blob_client(blob_name).get_blob_properties()
        return True
    except ResourceNotFoundError:
        return False
    except Exception as err:
        logging.warning(f"exists check failed for '{blob_name}': {err}")
        return False

def wait_for_copy(dest_blob, timeout_sec=300, poll_interval=5):
    props = dest_blob.get_blob_properties()
    status = getattr(props.copy, "status", None)
    elapsed = 0
    while status and status.lower() == "pending":
        if elapsed >= timeout_sec:
            raise TimeoutError(f"Copy timed out after {timeout_sec}s")
        time.sleep(poll_interval)
        elapsed += poll_interval
        props = dest_blob.get_blob_properties()
        status = getattr(props.copy, "status", None)
    return (status or "").lower(), props

def copy_blob(src_name, dst_name):
    src_url = container_src.get_blob_client(src_name).url
    dest = container_tgt.get_blob_client(dst_name)
    logging.info(f"Starting copy: '{src_name}' → '{dst_name}'")
    dest.start_copy_from_url(src_url)
    status, props = wait_for_copy(dest)
    if status != "success":
        raise RuntimeError(f"Copy failed ({status})")
    logging.info(f"Copied successfully: {src_name} → {dst_name} (id: {props.copy.id})")
    return props

def move_blob(src_name, dst_name):
    props = copy_blob(src_name, dst_name)
    logging.info(f"Deleting source blob '{src_name}'")
    container_src.get_blob_client(src_name).delete_blob()
    return props

# ─── 7. File Flow Logic ───────────────────────────────────────────────────────
now = datetime.now(timezone.utc)
result = "no action"

has_main = exists(container_src, path0)
has_temp = exists(container_src, temp0)
has_proc = exists(container_src, proc0)

logging.info(f"exists: main={has_main}, temp={has_temp}, processed={has_proc}")

if folderpath.rstrip("/").endswith("uploads") and has_main:
    move_blob(path0, temp0)
    copy_blob(temp0, deid0)
    result = "upload processed"

elif folderpath.rstrip("/").endswith("outbound") and has_temp:
    move_blob(temp0, proc0)
    result = "processed"

else:
    if not has_temp and not has_proc and now.day > 7:
        result = "1"
    elif has_temp and not has_proc:
        props = container_src.get_blob_client(temp0).get_blob_properties()
        if now - props.last_modified <= timedelta(days=7):
            result = "2"
    elif has_proc:
        result = "already processed"

logging.info(f"Final result: {result}")


# COMMAND ----------

