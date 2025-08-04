# Databricks notebook source
# MAGIC %pip install databricks-labs-dqx

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# # using PYPI package:
# %pip install databricks-labs-dqx

# # # using wheel file, DQX installed for the current user:
# # %pip install /Workspace/Users/<user-name>/.dqx/wheels/databricks_labs_dqx-*.whl

# # # using wheel file, DQX installed globally:
# # %pip install /Applications/dqx/wheels/databricks_labs_dqx-*.whl

# COMMAND ----------

csv_content = """CUSTOMER_ID,CUSTOMER_NAME,EMAIL,SEGMENT,COUNTRY,AGE,GENDER,AGE_GROUP
C001,Jesw,jessw@example.com,Premium,India,34,Male,30-39
C002,Abhiram,abhiram@example.com,Standard,India,28,Male,20-29
C003,Meera,meera@example.com,Premium,India,42,Female,40-49
C004,Ravi,ravi@example.com,Basic,Nepal,23,Male,20-29
C005,Anika,anika@example.com,Standard,Bangladesh,37,Female,30-39"""

dbutils.fs.put("/mnt/bronze/jerry/thing/customers_detailed.csv", csv_content, overwrite=True)
df = spark.read.csv("/mnt/bronze/jerry/thing/customers_detailed.csv", header=True, inferSchema=True)
df.write.format("delta").mode("overwrite").saveAsTable("stallion.learning.customers")

# COMMAND ----------

# DBTITLE 1,profiling a dataframe
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.profiler.dlt_generator import DQDltGenerator
from databricks.labs.dqx.engine import DQEngine


input_df = spark.read.table("stallion.learning.customers")

# profile input data
ws = WorkspaceClient()
profiler = DQProfiler(ws)
summary_stats, profiles = profiler.profile(input_df)

# generate DQX quality rules/checks
generator = DQGenerator(ws)
checks = generator.generate_dq_rules(profiles)  # with default level "error"

dq_engine = DQEngine(ws)

# save checks in arbitrary workspace location
dq_engine.save_checks_in_workspace_file(checks, workspace_path="/Shared/App1/checks.yml")

# generate Lakeflow Pipeline (DLT) expectations
dlt_generator = DQDltGenerator(ws)

dlt_expectations = dlt_generator.generate_dlt_rules(profiles, language="SQL")
print(dlt_expectations)

dlt_expectations = dlt_generator.generate_dlt_rules(profiles, language="Python")
print(dlt_expectations)

dlt_expectations = dlt_generator.generate_dlt_rules(profiles, language="Python_Dict")
print(dlt_expectations)

# COMMAND ----------

# DBTITLE 1,profiling a table
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient()
profiler = DQProfiler(ws)

summary_stats, profiles = profiler.profile_table(
    table="stallion.learning.customers",
    columns=["CUSTOMER_ID", "CUSTOMER_NAME", "EMAIL","SEGMENT","COUNTRY","AGE","GENDER","AGE_GROUP"],  # specify columns to profile 
    # CUSTOMER_ID,CUSTOMER_NAME,EMAIL,SEGMENT,COUNTRY,AGE,GENDER,AGE_GROUP
)

print("Summary Statistics:", summary_stats)
print("Generated Profiles:", profiles)

# COMMAND ----------

# DBTITLE 1,profiling multiple tables
# from databricks.labs.dqx.profiler.profiler import DQProfiler
# from databricks.sdk import WorkspaceClient

# ws = WorkspaceClient()
# profiler = DQProfiler(ws)

# # Profile several tables by name
# results = profiler.profile_tables(
#     tables=["main.data.table_001", "main.data.table_002"]
# )

# # Process results for each table
# for summary_stats, profiles in results:
#     print(f"Table statistics: {summary_stats}")
#     print(f"Generated profiles: {profiles}")

# # Profile several tables by wildcard patterns
# results = profiler.profile_tables(
#     patterns=["main.*", "data.*"]
# )

# # Process results for each table
# for summary_stats, profiles in results:
#     print(f"Table statistics: {summary_stats}")
#     print(f"Generated profiles: {profiles}")

# # Exclude tables matching specific patterns
# results = profiler.profile_tables(
#     patterns=["sys.*", "*_tmp"],
#     exclude_matched=True
# )

# # Process results for each table
# for summary_stats, profiles in results:
#     print(f"Table statistics: {summary_stats}")
#     print(f"Generated profiles: {profiles}")

# COMMAND ----------

# DBTITLE 1,profiling single df and table
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.sdk import WorkspaceClient

# Custom profiling options
custom_options = {
    # Sampling options
    "sample_fraction": 0.2,       # Sample 20% of the data
    "sample_seed": 42,            # Seed for reproducible sampling
    "limit": 2000,                # Limit to 2000 records after sampling
    
    # Outlier detection options
    "remove_outliers": True,      # Enable outlier detection for min/max rules
    "outlier_columns": ["price", "age"],  # Only detect outliers in specific columns
    "num_sigmas": 2.5,            # Use 2.5 standard deviations for outlier detection
    
    # Null value handling
    "max_null_ratio": 0.05,       # Generate is_not_null rule if <5% nulls
    
    # String handling
    "trim_strings": True,         # Trim whitespace from strings before analysis
    "max_empty_ratio": 0.02,      # Generate is_not_null_or_empty rule if <2% empty strings
    
    # Distinct value analysis
    "distinct_ratio": 0.01,       # Generate is_in rule if <1% distinct values
    "max_in_count": 20,           # Maximum items in is_in rule list
    
    # Value rounding
    "round": True,                # Round min/max values for cleaner rules
}

ws = WorkspaceClient()
profiler = DQProfiler(ws)

# Apply custom options for profiling a DataFrame
summary_stats, profiles = profiler.profile(input_df, options=custom_options)

# Apply custom options for profiling a table
summary_stats, profiles = profiler.profile_table(
    # table="catalog1.schema1.table1",
    # columns=["col1", "col2", "col3"],
    table="stallion.learning.customers",
    columns=["CUSTOMER_ID", "CUSTOMER_NAME", "EMAIL","SEGMENT","COUNTRY","AGE","GENDER","AGE_GROUP"],
    options=custom_options
)
# applying custom options for profiling a table display profiles
# display(profiles)

# COMMAND ----------

# DBTITLE 1,Profiling Options for multiple table
# from databricks.labs.dqx.profiler.profiler import DQProfiler
# from databricks.sdk import WorkspaceClient

# ws = WorkspaceClient()
# profiler = DQProfiler(ws)

# tables = [
#     "dqx.bronze.table_001",
#     "dqx.silver.table_001",
#     "dqx.silver.table_002",
# ]

# # Custom options with wildcard patterns
# custom_table_options = [
#     {
#         "table": "*",  # matches all tables by pattern
#         "options": {"sample_fraction": 0.5}
#     },
#     {
#         "table": "dqx.silver.*",  # matches tables in the 'dqx.silver' schema by pattern
#         "options": {"num_sigmas": 5}
#     },
#     {
#         "table": "dqx.silver.table_*",  # matches tables in 'dqx.silver' schema and having 'table_' prefix
#         "options": {"num_sigmas": 5}
#     },
#     {
#         "table": "dqx.silver.table_002",  # matches a specific table, overrides generic option
#         "options": {"sample_fraction": 0.1}
#     },
# ]

# # Profile multiple tables using custom options
# results = profiler.profile_tables(tables=tables, options=custom_table_options)

# # Profile multiple tables by wildcard patterns using custom options
# results = profiler.profile_tables(
#     patterns=["dqx.*"],
#     options=custom_table_options
# )

# COMMAND ----------

# from pyspark.sql.types import StructType, StructField, StringType, TimestampType, MapType
# from datetime import datetime

# schema = StructType([
#     StructField("run_config_name", StringType(), True),
#     StructField("check", StringType(), True),  # Stored as JSON string
#     StructField("created_at", TimestampType(), True)
# ])

# empty_df = spark.createDataFrame([], schema)

# empty_df.write.format("delta").saveAsTable("stallion.learning.checks_table")


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, MapType, ArrayType
from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.getOrCreate()

# Define schema for the checks table
check_struct = StructType([
    StructField("function", StringType(), True),
    StructField("arguments", MapType(StringType(), StringType()), True),
    StructField("for_each_column", ArrayType(StringType()), True)  # Optional
])

schema = StructType([
    StructField("name", StringType(), True),
    StructField("criticality", StringType(), True),
    StructField("check", check_struct, True),
    StructField("filter", StringType(), True),
    StructField("run_config_name", StringType(), True),
    StructField("user_metadata", MapType(StringType(), StringType()), True)
])

# Sample quality rules using Row for nested struct compatibility
checks_data = [
    Row(
        name="email_is_not_null",
        criticality="error",
        check=Row(function="is_not_null", arguments={"column": "EMAIL"}, for_each_column=None),
        filter=None,
        run_config_name="default",
        user_metadata={"source": "customer_validation"}
    ),
    Row(
        name="age_is_positive",
        criticality="warn",
        check=Row(function="greater_than", arguments={"column": "AGE", "value": "0"}, for_each_column=None),
        filter="COUNTRY = 'India'",
        run_config_name="default",
        user_metadata={"source": "customer_validation"}
    ),
    Row(
        name="segment_is_valid",
        criticality="error",
        check=Row(function="is_in", arguments={"column": "SEGMENT", "values": "Premium,Standard,Basic"}, for_each_column=None),
        filter=None,
        run_config_name="default",
        user_metadata={"source": "customer_validation"}
    )
]

# Create DataFrame
df = spark.createDataFrame(checks_data, schema=schema).withColumnRenamed('for_each_column', 'key')

# Save as Delta table
df.write.option("mergeSchema", "true").option("spark.databricks.delta.schema.autoMerge.enabled", "true").format("delta").mode("overwrite").saveAsTable("stallion.learning.checks_table1")

# COMMAND ----------

# DBTITLE 1,Create a Delta table (e.g., dq.config.checks_table)
# # from pyspark.sql import Row

# # checks_data = [
# #     Row(
# #         name="email_is_not_null",
# #         criticality="error",
# #         check={"function": "is_not_null", "arguments": {"column": "EMAIL"}},
# #         filter=None,
# #         run_config_name="default",
# #         user_metadata={"source": "customer_validation"}
# #     ),
# #     Row(
# #         name="age_is_positive",
# #         criticality="warn",
# #         check={"function": "greater_than", "arguments": {"column": "AGE", "value": 0}},
# #         filter="COUNTRY = 'India'",
# #         run_config_name="default",
# #         user_metadata={"source": "customer_validation"}
# #     ),
# #     Row(
# #         name="segment_is_valid",
# #         criticality="error",
# #         check={"function": "is_in", "arguments": {"column": "SEGMENT", "values": ["Premium", "Standard", "Basic"]}},
# #         filter=None,
# #         run_config_name="default",
# #         user_metadata={"source": "customer_validation"}
# #     )
# # ]

# # spark.createDataFrame(checks_data).write.format("delta").mode("overwrite").saveAsTable("dq.config.checks_table")
# from pyspark.sql.types import StructType, StructField, StringType, MapType
# from pyspark.sql import SparkSession

# # Define schema for the checks table
# schema = StructType([
#     StructField("name", StringType(), True),
#     StructField("criticality", StringType(), True),
#     StructField("check", StructType([
#         StructField("function", StringType(), True),
#         StructField("arguments", MapType(StringType(), StringType()), True)
#     ]), True),
#     StructField("filter", StringType(), True),
#     StructField("run_config_name", StringType(), True),
#     StructField("user_metadata", MapType(StringType(), StringType()), True)
# ])

# # Sample quality rules
# checks_data = [
#     {
#         "name": "email_is_not_null",
#         "criticality": "error",
#         "check": {
#             "function": "is_not_null",
#             "arguments": {"column": "EMAIL"}
#         },
#         "filter": None,
#         "run_config_name": "default",
#         "user_metadata": {"source": "customer_validation"}
#     },
#     {
#         "name": "age_is_positive",
#         "criticality": "warn",
#         "check": {
#             "function": "greater_than",
#             "arguments": {"column": "AGE", "value": "0"}
#         },
#         "filter": "COUNTRY = 'India'",
#         "run_config_name": "default",
#         "user_metadata": {"source": "customer_validation"}
#     },
#     {
#         "name": "segment_is_valid",
#         "criticality": "error",
#         "check": {
#             "function": "is_in",
#             "arguments": {"column": "SEGMENT", "values": "Premium,Standard,Basic"}
#         },
#         "filter": None,
#         "run_config_name": "default",
#         "user_metadata": {"source": "customer_validation"}
#     }
# ]

# # Create DataFrame
# df = spark.createDataFrame(checks_data, schema=schema)

# # Save as Delta table
# df.write.format("delta").mode("overwrite").saveAsTable("stallion.learning.checks_table1")
# # dq_engine.save_checks_in_table(checks_data, table_name="stallion.learning.checks_table1", run_config_name="workflow_001", mode="overwrite")

# COMMAND ----------

# spark.table("stallion.learning.checks_table1").printSchema()

# COMMAND ----------

spark.table("stallion.learning.checks_table1").select("check.*").show(truncate=False)


# COMMAND ----------

checks = [
  {
    "criticality": "error",
    "check": {"function": "is_not_null", "for_each_column": ["col1", "col2"], "arguments": {}},
    "filter": "col1 > 0",
    "user_metadata": {"check_owner": "someone@email.com"},
  },
  {
    "name": "column_not_less_than",
    "criticality": "warn",
    "check": {"function": "is_not_less_than", "arguments": {"column": "col_2", "limit": 1}},
  },
  {
    "criticality": "warn",
    "name": "column_in_list",
    "check": {"function": "is_in_list", "arguments": {"column": "col_2", "allowed": [1, 2]}},
  },
]

dq_engine = DQEngine(WorkspaceClient())

# save checks to a delta table
dq_engine.save_checks_in_table(checks, "main.default.dqx_checks_table", run_config_name="default", mode="overwrite")

# load checks as dict from the delta table
checks = dq_engine.load_checks_from_table("main.default.dqx_checks_table")

# validate loaded checks
assert not dq_engine.validate_checks(checks).has_errors
```

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

# Initialize DQEngine
dq_engine = DQEngine(WorkspaceClient())

# Load checks from your Delta table
checks = dq_engine.load_checks_from_table(table_name="stallion.learning.checks_table1")

# Validate syntax
dq_engine.validate_checks_syntax(checks)

# COMMAND ----------

# from databricks.labs.dqx.engine import DQEngine
# from databricks.sdk import WorkspaceClient

# # dq_engine = DQEngine(WorkspaceClient())
# # checks = dq_engine.load_checks_from_table("stallion.learning.checks_table1", run_config_name="default")
# # JSONDecodeError: Expecting value: line 1 column 1 (char 0)
# # from databricks.labs.dqx.engine import DQEngine

# raw_checks = spark.table("stallion.learning.checks_table1").collect()
# checks_list = [row.asDict() for row in raw_checks]

# dq_engine = DQEngine(WorkspaceClient())
# dq_engine.validate_checks(checks_list)
# # Recreate the table with clean JSON
# from pyspark.sql import Row

# def clean_json(row):
#     # Implement your JSON cleaning logic here
#     # For example, ensure 'check' column is valid JSON
#     import json
#     try:
#         json.loads(row['check'])
#         return True
#     except Exception:
#         return False

# cleaned_checks = [Row(**row) for row in checks_list if clean_json(row)]
# checks_df_fixed = spark.createDataFrame(cleaned_checks)
# checks_df_fixed.write.format("delta").mode("overwrite").saveAsTable("stallion.learning.checks_table1")
# # checks_df_fixed.write.format("delta").mode("overwrite").saveAsTable("stallion.learning.checks_table1")
# # checks = dq_engine.load_checks_from_table("stallion.learning.checks_table1", run_config_name="default")


# COMMAND ----------

from databricks.labs.dqx.config import OutputConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

input_df = spark.read.table("stallion.learning.customers")


dq_engine = DQEngine(WorkspaceClient())
checks = dq_engine.load_checks_from_table(table_name="stallion.learning.checks_table1")

valid_df, quarantine_df = dq_engine.apply_checks_by_metadata_and_split(input_df, checks)

dq_engine.save_results_in_table(
    output_df=valid_df,
    quarantine_df=quarantine_df,
    output_config=OutputConfig("stallion.learning.customers_valid"),
    quarantine_config=OutputConfig("stallion.learning.customers_quarantine")
)

annotated_df = dq_engine.apply_checks_by_metadata(input_df, checks)

dq_engine.save_results_in_table(
    output_df=annotated_df,
    output_config=OutputConfig("stallion.learning.customers_annotated")
)

# COMMAND ----------

from databricks.labs.dqx.config import OutputConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

dq_engine = DQEngine(WorkspaceClient())

# Load checks from the "default" run config
default_checks = dq_engine.load_checks_from_table(table_name="stallion.learning.checks_table1")

# Load checks from the "workflow_001" run config
workflow_checks = dq_engine.load_checks_from_table(table_name="stallion.learning.checks_table1", run_config_name="workflow_001")

# Load checks from the installation (from a table defined in 'checks_table' in the run config)
# Only works if DQX is installed in the workspace
workflow_checks = dq_engine.load_checks_from_installation(method="table", assume_user=True, run_config_name="workflow_001")

checks = default_checks + workflow_checks

input_df = spark.read.table("stallion.learning.customers")

# Option 1: apply quality rules on the dataframe and provide valid and invalid (quarantined) dataframes
valid_df, quarantine_df = dq_engine.apply_checks_by_metadata_and_split(input_df, checks)
dq_engine.save_results_in_table(
  output_df=valid_df, 
  quarantine_df=quarantine_df, 
  output_config=OutputConfig("stallion.learning.customersvaliddata"), 
  quarantine_config=OutputConfig("catalog.schema.customersquarantine_data")
)

# Option 2: apply quality rules on the dataframe and report issues as additional columns (`_warning` and `_error`)
valid_and_quarantine_df = dq_engine.apply_checks_by_metadata(input_df, checks)
dq_engine.save_results_in_table(
  output_df= valid_and_quarantine_df, 
  output_config=OutputConfig("stallion.learning.customersvalid_and_quarantine_data")
)

# COMMAND ----------

# DBTITLE 1,datachecks
# import yaml
# from databricks.labs.dqx.engine import DQEngine
# from databricks.sdk import WorkspaceClient

# dq_engine = DQEngine(WorkspaceClient())

# # Checks can be defined in code as below or generated by the profiler
# # Must be defined as list[dict]
# checks = yaml.safe_load("""
# - criticality: warn
#   check:
#     function: is_not_null_and_not_empty
#     arguments:
#       column: col3
#  # ...
# """)

# # save checks in a local path
# # always overwrite the file
# dq_engine.save_checks_in_local_file(checks, path="checks.yml")

# # save checks in arbitrary workspace location
# # always overwrite the file
# dq_engine.save_checks_in_workspace_file(checks, workspace_path="/Shared/App1/checks.yml")

# # save checks in file defined in 'checks_file' in the run config
# # always overwrite the file
# # only works if DQX is installed in the workspace
# # dq_engine.save_checks_in_installation(checks, method="file", assume_user=True, run_config_name="default")

# # save checks in a Delta table with default run config for filtering
# # append checks in the table
# dq_engine.save_checks_in_table(checks, table_name="stallion.learning.checks_table_new", mode="append")

# # save checks in a Delta table with specific run config for filtering
# # overwrite checks in the table for the given run config
# # dq_engine.save_checks_in_table(checks, table_name="dq.config.checks_table", run_config_name="workflow_001", mode="overwrite")
# # dq_engine.save_checks_in_table(checks, table_name="stallion.learning.checks_table1", run_config_name="workflow_001", mode="overwrite")


# # save checks in table defined in 'checks_table' in the run config
# # always overwrite checks in the table for the given run config
# # only works if DQX is installed in the workspace
# # dq_engine.save_checks_in_installation(checks, method="table", assume_user=True, run_config_name="default")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stallion.learning.checks_table1

# COMMAND ----------

# DBTITLE 1,for largesets
# For large datasets, use aggressive sampling
large_dataset_opts = {
    "sample_fraction": 0.01,  # Sample only 1% for very large datasets
    "limit": 10000,          # Increase limit for better statistical accuracy
    "sample_seed": 42,       # Use consistent seed for reproducible results
}

# For medium datasets, use moderate sampling
medium_dataset_opts = {
    "sample_fraction": 0.1,   # Sample 10%
    "limit": 5000,           # Reasonable limit
}

# For small datasets, disable sampling
small_dataset_opts = {
    "sample_fraction": None,  # Use all data
    "limit": None,           # No limit
}

# COMMAND ----------

# DBTITLE 1,dq rules defined in file level
# from databricks.labs.dqx.engine import DQEngine
# from databricks.sdk import WorkspaceClient

# dq_engine = DQEngine(WorkspaceClient())

# # Load check from the installation (from file defined in 'checks_file' in the run config)
# # Only works if DQX is installed in the workspace
# default_checks = dq_engine.load_checks_from_installation(method="file", assume_user=True, run_config_name="default")
# workflow_checks = dq_engine.load_checks_from_installation(method="file", assume_user=True, run_config_name="workflow_001")
# checks = default_checks + workflow_checks

# input_df = spark.read.table("catalog1.schema1.table1")

# # Option 1: apply quality rules on the dataframe and provide valid and invalid (quarantined) dataframes
# valid_df, quarantine_df = dq_engine.apply_checks_by_metadata_and_split(input_df, checks)
# dq_engine.save_results_in_table(output_df=valid_df, quarantine_df=quarantine_df, run_config_name="default")

# # Option 2: apply quality rules on the dataframe and report issues as additional columns (`_warning` and `_error`)
# valid_and_quarantine_df = dq_engine.apply_checks_by_metadata(input_df, checks)
# dq_engine.save_results_in_table(output_df=valid_and_quarantine_df, run_config_name="default")

# from databricks.labs.dqx.config import OutputConfig
# from databricks.labs.dqx.engine import DQEngine
# from databricks.sdk import WorkspaceClient

# dq_engine = DQEngine(WorkspaceClient())

# # Load checks from multiple files and merge
# default_checks = dq_engine.load_checks_from_workspace_file("/Shared/App1/default_checks.yml")
# workflow_checks = dq_engine.load_checks_from_workspace_file("/Shared/App1/workflow_checks.yml")
# checks = default_checks + workflow_checks

# input_df = spark.read.table("catalog1.schema1.table1")

# # Loading and Applying Checks Defined in a Workspace File
# # Option 1: apply quality rules on the dataframe and provide valid and invalid (quarantined) dataframes
# valid_df, quarantine_df = dq_engine.apply_checks_by_metadata_and_split(input_df, checks)
# dq_engine.save_results_in_table(
#   output_df=valid_df, 
#   quarantine_df=quarantine_df, 
#   output_config=OutputConfig("catalog.schema.valid_data"), 
#   quarantine_config=OutputConfig("catalog.schema.quarantine_data")
# )

# # Option 2: apply quality rules on the dataframe and report issues as additional columns (`_warning` and `_error`)
# valid_and_quarantine_df = dq_engine.apply_checks_by_metadata(input_df, checks)
# dq_engine.save_results_in_table(
#   output_df=valid_df, 
#   quarantine_df=quarantine_df, 
#   output_config=OutputConfig("catalog.schema.valid_and_quarantine_data")
# )
# # 
# from databricks.labs.dqx.config import OutputConfig
# from databricks.labs.dqx.engine import DQEngine
# from databricks.sdk import WorkspaceClient

# dq_engine = DQEngine(WorkspaceClient())

# # Load checks from multiple files and merge
# default_checks = DQEngine.load_checks_from_local_file("default_checks.yml")
# workflow_checks = DQEngine.load_checks_from_local_file("workflow_checks.yml")
# checks = default_checks + workflow_checks

# input_df = spark.read.table("catalog1.schema1.table1")

# # Option 1: apply quality rules on the dataframe and provide valid and invalid (quarantined) dataframes
# valid_df, quarantine_df = dq_engine.apply_checks_by_metadata_and_split(input_df, checks)
# dq_engine.save_results_in_table(
#   output_df=valid_df, 
#   quarantine_df=quarantine_df, 
#   output_config=OutputConfig("catalog.schema.valid_data"), 
#   quarantine_config=OutputConfig("catalog.schema.quarantine_data")
# )

# # Option 2: apply quality rules on the dataframe and report issues as additional columns (`_warning` and `_error`)
# valid_and_quarantine_df = dq_engine.apply_checks_by_metadata(input_df, checks)
# dq_engine.save_results_in_table(
#   output_df=valid_and_quarantine_df,
#   output_config=OutputConfig("catalog.schema.valid_and_quarantine_data")
# )

# COMMAND ----------

# DBTITLE 1,Applying Quality Rules defined in a Delta Table
from databricks.labs.dqx.config import OutputConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

dq_engine = DQEngine(WorkspaceClient())

# Load checks from the "default" run config
default_checks = dq_engine.load_checks_from_table(table_name="stallion.learning.checks_table1")

# # Load checks from the "workflow_001" run config
# workflow_checks = dq_engine.load_checks_from_table(table_name="stallion.learning.checks_table1", run_config_name="workflow_001")

# # Load checks from the installation (from a table defined in 'checks_table' in the run config)
# # Only works if DQX is installed in the workspace
# workflow_checks = dq_engine.load_checks_from_installation(method="table", assume_user=True, run_config_name="workflow_001")

checks = default_checks + workflow_checks

input_df = spark.read.table("stallion.learning.customers")

# Option 1: apply quality rules on the dataframe and provide valid and invalid (quarantined) dataframes
valid_df, quarantine_df = dq_engine.apply_checks_by_metadata_and_split(input_df, checks)
dq_engine.save_results_in_table(
  output_df=valid_df, 
  quarantine_df=quarantine_df, 
  output_config=OutputConfig("stallion.learning.customersvaliddata"), 
  quarantine_config=OutputConfig("stallion.learning.customersquarantine_data")
)

# Option 2: apply quality rules on the dataframe and report issues as additional columns (`_warning` and `_error`)
valid_and_quarantine_df = dq_engine.apply_checks_by_metadata(input_df, checks)
dq_engine.save_results_in_table(
  output_df= valid_and_quarantine_df, 
  output_config=OutputConfig("stallion.learning.customersvalid_and_quarantine_data")
)

# COMMAND ----------

# DBTITLE 1,working scenario datachecks
from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.config import OutputConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule, DQDatasetRule, DQForEachColRule
from databricks.sdk import WorkspaceClient


dq_engine = DQEngine(WorkspaceClient())

checks = [
  *DQForEachColRule(  # check for multiple columns applied individually
    columns= ["CUSTOMER_ID", "CUSTOMER_NAME", "EMAIL","SEGMENT","COUNTRY","AGE","GENDER","AGE_GROUP"],
    criticality="error",
    check_func=check_funcs.is_not_null).get_rules(),

  DQRowRule(  # check with a filter
    name="col_4_is_null_or_empty",
    criticality="warn",
    filter="AGE < 3",
    check_func=check_funcs.is_not_null_and_not_empty,
    column="AGE",
  ),

  DQRowRule(  # check with user metadata
    name="col_5_is_null_or_empty",
    criticality="warn",
    check_func=check_funcs.is_not_null_and_not_empty,
    column="EMAIL",
    user_metadata={
      "check_type": "completeness",
      "responsible_data_steward": "someone@email.com"
    },
  )
]

input_df = spark.read.table("stallion.learning.customers")

# Option 1: apply quality rules on the dataframe and provide valid and invalid (quarantined) dataframes
valid_df, quarantine_df = dq_engine.apply_checks_and_split(input_df, checks)
dq_engine.save_results_in_table(
  output_df=valid_df, 
  quarantine_df=quarantine_df, 
  output_config=OutputConfig("stallion.learning.cusvaliddata"), 
  quarantine_config=OutputConfig("stallion.learning.cusquarantine_data")
)

# Option 2: apply quality rules on the dataframe and report issues as additional columns (`_warning` and `_error`)
valid_and_quarantine_df = dq_engine.apply_checks(input_df, checks)
dq_engine.save_results_in_table(
  output_df=valid_and_quarantine_df, 
  output_config=OutputConfig("stallion.learning.cusvalid_and_quarantine_data")
)


# COMMAND ----------

input_df.show()
quarantine_df.show()
valid_and_quarantine_df.show()
valid_df.show()

# COMMAND ----------

from inspect import getsource
print(getsource(DQRowRule))

# COMMAND ----------

# DBTITLE 1,End-to-end Quality Checking
from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.config import InputConfig, OutputConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule
from databricks.sdk import WorkspaceClient

dq_engine = DQEngine(WorkspaceClient())

# Define some checks using DQRule classes
checks = [
 DQRowRule(
        name="id_not_null",
        criticality="error",
        check_func=check_funcs.is_not_null,
        column="CUSTOMER_ID",
    ),
    DQRowRule(
        name="AGE_positive",
        criticality="warn",
        check_func=check_funcs.is_not_null_and_not_empty,
        column="AGE"
    ),
]

# Option 1: Split data and write to separate output tables (validated and quarantined data)
dq_engine.apply_checks_and_save_in_table(
    checks=checks,
    input_config=InputConfig("stallion.learning.customers"),
    output_config=OutputConfig("stallion.learning.valid_data"), 
    quarantine_config=OutputConfig("stallion.learning.quarantine_data")
)

# Option 2: Write all data with error/warning columns to a single output table
dq_engine.apply_checks_and_save_in_table(
    checks=checks,
    input_config=InputConfig("stallion.learning.customers"),
    output_config=OutputConfig("stallion.learning.valid_and_quarantine_data"),
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stallion.learning.valid_and_quarantine_data

# COMMAND ----------

# DBTITLE 1,streaming
# # Read from Delta as a streaming source, apply checks, and write outputs to a Delta table
# dq_engine.apply_checks_and_save_in_table(
#     checks=checks,
#     input_config=InputConfig(
#       "catalog.schema.input_data", 
#       is_streaming=True
#     ),
#     output_config=OutputConfig(
#       "catalog.schema.valid_and_quarantine_data",
#       trigger={"availableNow": True},
#       options={"checkpointLocation": "/path/to/checkpoint", "mergeSchema": "true"}
#     ),
# )

# COMMAND ----------

# DBTITLE 1,working scenario datacheck
from pyspark.sql.functions import lit, struct
from pyspark.sql.types import StructType, StructField, StringType, MapType

# Define the schema for the quality rules
schema = StructType([
    StructField("name", StringType(), False),
    StructField("criticality", StringType(), False),
    StructField("check", StructType([
        StructField("function", StringType(), False),
        StructField("arguments", MapType(StringType(), StringType()), False)
    ]), False),
    StructField("filter", StringType(), True),
    StructField("run_config_name", StringType(), True),
    StructField("user_metadata", MapType(StringType(), StringType()), True)
])

# Sample data for the quality rules
data = [
    (
        "is_not_null_customer_id", # Rule name 
        "error", # Criticality
        {"function": "is_not_null", "arguments": {"column": "CUSTOMER_ID"}}, # Check function and arguments
        None, # Filter
        "default", # Run config name
        {} # User metadata
    ),
    (
        "is_not_null_customer_name",
        "error",
        {"function": "is_not_null", "arguments": {"column": "CUSTOMER_NAME"}},
        None,
        "default",
        {}
    ),
    (
        "is_not_null_email",
        "error",
        {"function": "is_not_null", "arguments": {"column": "EMAIL"}},
        None,
        "default",
        {}
    ),
    (
        "is_not_null_segment",
        "error",
        {"function": "is_not_null", "arguments": {"column": "SEGMENT"}},
        None,
        "default",
        {}
    ),
    (
        "is_not_null_country",
        "error",
        {"function": "is_not_null", "arguments": {"column": "COUNTRY"}},
        None,
        "default",
        {}
    ),
    (
        "is_not_null_age",
        "error",
        {"function": "is_not_null", "arguments": {"column": "AGE"}},
        None,
        "default",
        {}
    ),
    (
        "is_not_null_gender",
        "error",
        {"function": "is_not_null", "arguments": {"column": "GENDER"}},
        None,
        "default",
        {}
    ),
    (
        "is_not_null_age_group",
        "error",
        {"function": "is_not_null", "arguments": {"column": "AGE_GROUP"}},
        None,
        "default",
        {}
    ),
    (
        "age_must_be_positive",
        "error",
        {"function": "is_in_range", "arguments": {"column": "AGE", "min": "0", "max": "100"}},
        None,
        "default",
        {}
    )
]

rules_df = spark.createDataFrame(data, schema)

# Write the DataFrame to a Delta table
rules_df.write.format("delta").mode("overwrite").saveAsTable("customer_quality_rules")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_quality_rules

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.config import OutputConfig
from databricks.sdk import WorkspaceClient

# Initialize the DQEngine
dq_engine = DQEngine(WorkspaceClient())

# Load the quality rules from the Delta table
rules_df = spark.table("customer_quality_rules")
rules = rules_df.collect()

# Prepare the checks list
checks = []
for row in rules:    from databricks.labs.dqx.engine import DQRule
check = DQRule(
        name=row["name"],
        criticality=row["criticality"],
        check=row["check"],
        filter=row["filter"],
        run_config_name=row["run_config_name"],
        user_metadata=row["user_metadata"]
    )
checks.append(check)

check = {
        "name": row["name"],
        "criticality": row["criticality"],
        "check": row["check"].asDict(),
        "filter": row["filter"],
        "run_config_name": row["run_config_name"],
        "user_metadata": row["user_metadata"]
    }
checks.append(check)

# Load the customer data
input_df = spark.read.table("stallion.learning.customers")

# Apply the quality checks
valid_df, quarantine_df = dq_engine.apply_checks_and_split(input_df, checks)

# Save the results
dq_engine.save_results_in_table(
    output_df=valid_df,
    quarantine_df=quarantine_df,
    output_config=OutputConfig("stallion.learning.cusvaliddata17"),
    quarantine_config=OutputConfig("stallion.learning.cusquarantine_data17")
)


# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.config import OutputConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule, DQForEachColRule,DQDatasetRule

# Initialize DQEngine
dq_engine = DQEngine(WorkspaceClient())

# Define quality checks
checks = [
DQRowRule(  # check with a filter
    name="col_4_is_null_or_empty",
    criticality="warn",
    filter="AGE < 0",
    check_func=check_funcs.is_not_null_and_not_empty,
    column="AGE",
  ),

  DQRowRule(  # check with user metadata
    name="col_5_is_null_or_empty",
    criticality="warn",
    check_func=check_funcs.is_not_null_and_not_empty,
    column="EMAIL",
    user_metadata={
      "check_type": "completeness",
      "responsible_data_steward": "someone@email.com"
    },
  ),
    *DQForEachColRule(
        columns=["CUSTOMER_ID", "CUSTOMER_NAME", "EMAIL", "SEGMENT", "COUNTRY", "AGE", "GENDER", "AGE_GROUP"],
        criticality="error",
        check_func=check_funcs.is_not_null
    ).get_rules()
]
additional_checks = [
    DQDatasetRule(
        name="customer_id_unique",
        criticality="error",
        check_func=check_funcs.is_unique,
        columns=["CUSTOMER_ID"]
    ),
    DQRowRule(
        name="age_group_consistency",
        criticality="error",
        check_func=check_funcs.regex_match,
        column="AGE_GROUP",
        regex=r"^\d{1,2}-\d{1,2}$"
    ),
    DQRowRule(
        name="gender_validity",
        criticality="error",
        check_func=check_funcs.value_is_in_list,
        column="GENDER",
        allowed=["M", "F", "Other"]
    ),
    DQRowRule(
        name="email_domain_check",
        criticality="warn",
        check_func=check_funcs.regex_match,
        column="EMAIL",
        regex=r"@company\.com$"
    ),
    DQRowRule(
        name="country_validity",
        criticality="error",
        check_func=check_funcs.value_is_in_list,
        column="COUNTRY",
        allowed=["India", "USA", "UK", "Canada"]
    ),
    DQRowRule(
        name="age_group_range_check",
        criticality="error",
        check_func=check_funcs.is_in_range,
        column="AGE",
        min_limit=18,
        max_limit=25
    ),
    DQRowRule(
        name="segment_not_null_or_empty",
        criticality="error",
        check_func=check_funcs.is_not_null_and_not_empty,
        column="SEGMENT"
    ),
    DQRowRule(
        name="country_not_null_or_empty",
        criticality="error",
        check_func=check_funcs.is_not_null_and_not_empty,
        column="COUNTRY"
    ),
    DQRowRule(
        name="gender_not_null_or_empty",
        criticality="error",
        check_func=check_funcs.is_not_null_and_not_empty,
        column="GENDER"
    ),
    DQRowRule(
        name="age_group_not_null_or_empty",
        criticality="error",
        check_func=check_funcs.is_not_null_and_not_empty,
        column="AGE_GROUP"
    )
]
all_checks = checks + additional_checks


# COMMAND ----------

# from databricks.labs.dqx import check_funcs
# from databricks.labs.dqx.rule import DQRowRule, DQForEachColRule,DQDatasetRule

# additional_checks = [
#     DQDatasetRule( #TypeError: DQDatasetRule.__init__() got an unexpected keyword argument 'arguments'
#     name="email_format_check",
#     criticality="warn",
#     check_func=check_funcs.regex_match,
#     columns=["EMAIL"],
#     arguments={"regex": r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"} 
# ),
#     DQRowRule(
#         name="gender_validity",
#         criticality="error",
#         check_func=check_funcs.value_is_in_list,
#         column="GENDER",
#         arguments={"allowed": ["M", "F", "Other"]}
#     ),
#     DQRowRule(
#         name="email_domain_check",
#         criticality="warn",
#         check_func=check_funcs.regex_match,
#         column="EMAIL",
#         arguments={"regex": r"@company\.com$"}
#     ),
#     DQRowRule(
#         name="country_validity",
#         criticality="error",
#         check_func=check_funcs.value_is_in_list,
#         column="COUNTRY",
#         arguments={"allowed": ["India", "USA", "UK", "Canada"]}
#     ),
#     DQRowRule(
#         name="age_group_range_check",
#         criticality="error",
#         check_func=check_funcs.is_in_range,
#         column="AGE",
#         arguments={"min_limit": 18, "max_limit": 25}
#     ),
#     DQRowRule(
#         name="segment_not_null_or_empty",
#         criticality="error",
#         check_func=check_funcs.is_not_null_and_not_empty,
#         column="SEGMENT"
#     ),
#     DQRowRule(
#         name="country_not_null_or_empty",
#         criticality="error",
#         check_func=check_funcs.is_not_null_and_not_empty,
#         column="COUNTRY"
#     ),
#     DQRowRule(
#         name="gender_not_null_or_empty",
#         criticality="error",
#         check_func=check_funcs.is_not_null_and_not_empty,
#         column="GENDER"
#     ),
#     DQRowRule(
#         name="age_group_not_null_or_empty",
#         criticality="error",
#         check_func=check_funcs.is_not_null_and_not_empty,
#         column="AGE_GROUP"
#     )
# ]


# COMMAND ----------

valid_df, quarantine_df = dq_engine.apply_checks_and_split(input_df, all_checks)
dq_engine.save_results_in_table(
    output_df=valid_df,
    quarantine_df=quarantine_df,
    output_config=OutputConfig("stallion.learning.cusvaliddata1"),
    quarantine_config=OutputConfig("stallion.learning.cusquarantine_data1")
)

# COMMAND ----------

# Load customer data
input_df = spark.read.table("stallion.learning.customers")

# Apply quality checks and split data into valid and quarantined datasets
valid_df, quarantine_df = dq_engine.apply_checks_and_split(input_df, checks)

# Save results to Delta tables
dq_engine.save_results_in_table(
    output_df=valid_df,
    quarantine_df=quarantine_df,
    output_config=OutputConfig("stallion.learning.cusvaliddata"),
    quarantine_config=OutputConfig("stallion.learning.cusquarantine_data")
)


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, MapType

# Define schema for quality rules
schema = StructType([
    StructField("name", StringType(), nullable=False),
    StructField("criticality", StringType(), nullable=False),
    StructField("check", StructType([
        StructField("function", StringType(), nullable=False),
        StructField("arguments", MapType(StringType(), StringType()), nullable=True)
    ]), nullable=False),
    StructField("filter", StringType(), nullable=True),
    StructField("run_config_name", StringType(), nullable=True),
    StructField("user_metadata", MapType(StringType(), StringType()), nullable=True)
])

# Example data for quality rules
data = [
    ("is_not_null_customer_id", "error", {"function": "is_not_null", "arguments": {"column": "CUSTOMER_ID"}}, None, "default", None),
    ("age_must_be_positive", "error", {"function": "is_in_range", "arguments": {"column": "AGE", "min_limit": "0", "max_limit": "100"}}, None, "default", None)
]

# Create DataFrame for quality rules
rules_df = spark.createDataFrame(data, schema)

# Save to Delta table
rules_df.write.format("delta").mode("overwrite").saveAsTable("stallion.learning.customer_quality_rules1")


# COMMAND ----------

# Load quality rules from Delta table
rules_df = spark.read.table("stallion.learning.customer_quality_rules")

# Convert rules to DQX format (this step depends on your implementation)
# For example, you might need to transform the rules_df into a list of DQRowRule or DQForEachColRule objects

# Apply quality checks to customer data
valid_df, quarantine_df = dq_engine.apply_checks_and_split(input_df, checks)

# Save results
dq_engine.save_results_in_table(
    output_df=valid_df,
    quarantine_df=quarantine_df,
    output_config=OutputConfig("stallion.learning.cusvaliddata"),
    quarantine_config=OutputConfig("stallion.learning.cusquarantine_data")
)


# COMMAND ----------

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.rule import DQRowRule

additional_checks = [
    DQRowRule(
        name="customer_id_unique",
        criticality="error",
        check_func=check_funcs.is_unique,
        column="CUSTOMER_ID",
    ),
    DQRowRule(
        name="age_group_consistency",
        criticality="error",
        check_func=check_funcs.regex_match,
        column="AGE_GROUP",
        arguments={"regex": r"^\d{1,2}-\d{1,2}$"},
    ),
    DQRowRule(
        name="gender_validity",
        criticality="error",
        check_func=check_funcs.value_is_in_list,
        column="GENDER",
        arguments={"allowed": ["M", "F", "Other"]},
    ),
    DQRowRule(
        name="email_domain_check",
        criticality="warn",
        check_func=check_funcs.regex_match,
        column="EMAIL",
        arguments={"regex": r"@company\.com$"},
    ),
    DQRowRule(
        name="country_validity",
        criticality="error",
        check_func=check_funcs.value_is_in_list,
        column="COUNTRY",
        arguments={"allowed": ["India", "USA", "UK", "Canada"]},
    ),
    DQRowRule(
        name="age_group_range_check",
        criticality="error",
        check_func=check_funcs.is_in_range,
        column="AGE",
        arguments={"min_limit": 18, "max_limit": 25},
    ),
    DQRowRule(
        name="segment_not_null_or_empty",
        criticality="error",
        check_func=check_funcs.is_not_null_and_not_empty,
        column="SEGMENT",
    ),
    DQRowRule(
        name="country_not_null_or_empty",
        criticality="error",
        check_func=check_funcs.is_not_null_and_not_empty,
        column="COUNTRY",
    ),
    DQRowRule(
        name="gender_not_null_or_empty",
        criticality="error",
        check_func=check_funcs.is_not_null_and_not_empty,
        column="GENDER",
    ),
    DQRowRule(
        name="age_group_not_null_or_empty",
        criticality="error",
        check_func=check_funcs.is_not_null_and_not_empty,
        column="AGE_GROUP",
    ),
]
all_checks = checks + additional_checks
