# Databricks notebook source
import glob
import os

user_name = spark.sql("select current_user() as user").collect()[0]["user"]
dqx_wheel_files = glob.glob(f"/Workspace/Users/{user_name}/.dqx/wheels/databricks_labs_dqx-*.whl")
dqx_latest_wheel = max(dqx_wheel_files, key=os.path.getctime) if dqx_wheel_files else None
%pip install {dqx_latest_wheel}
%restart_python

# COMMAND ----------

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
C005,Anika,anika@example.com,Standard,Bangladesh,37,Female,30-39
C006,Upen,anikathing,Standard,Bangladesh,-1,Female,30-39
C007,NullName,,Premium,India,25,,20-29
C008,TestUser,testuser@example.com,Standard,India,-5,Gay,20-29
C009,EmptyAge,empty@example.com,Basic,Nepal,,Male,
C010,InvalidGender,invalid@example.com,Premium,India,45,Gay,40-49
C011,NoEmail,,Basic,Nepal,31,Male,30-39
C012,NegativeAge,neg@example.com,Standard,India,-10,Female,10-19
C013,InvalidSegment,invseg@example.com,Unknown,India,29,Male,20-29
C014,NullCountry,nullcountry@example.com,Premium,,38,Female,30-39
C015,EmptyGender,emptygender@example.com,Standard,India,27,,20-29
C016,InvalidAgeGroup,invalidag@example.com,Basic,Nepal,22,Male,Unknown
C017,NullAll,,,,,,,
C018,NegativeAge2,neg2@example.com,Standard,Bangladesh,-20,Male,10-19
C019,InvalidGender2,invgen2@example.com,Premium,India,33,Alien,30-39
C020,EmptySegment,emptyseg@example.com,,India,40,Female,40-49
"""

dbutils.fs.put("/mnt/bronze/jerry/thing/customers_detailed_n_test.csv", csv_content, overwrite=True)
df = spark.read.csv("/mnt/bronze/jerry/thing/customers_detailed_n_test.csv", header=True, inferSchema=True)
df.write.format("delta").mode("overwrite").saveAsTable("stallion.learning.customers_n_test_1")

# COMMAND ----------

# DBTITLE 1,profiling a dataframe
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.profiler.dlt_generator import DQDltGenerator
from databricks.labs.dqx.engine import DQEngine


input_df = spark.read.table("stallion.learning.customers_n_test_1")

# profile input data
ws = WorkspaceClient() #
profiler = DQProfiler(ws)
summary_stats, profiles = profiler.profile(input_df)

# generate DQX quality rules/checks
generator = DQGenerator(ws)
checks = generator.generate_dq_rules(profiles)  # with default level "error"

dq_engine = DQEngine(ws)

# save checks in arbitrary workspace location
dq_engine.save_checks_in_workspace_file(checks, workspace_path="/Shared/App1/checks_n_test_18.yml")

# generate Lakeflow Pipeline (DLT) expectations
dlt_generator = DQDltGenerator(ws)

dlt_expectations = dlt_generator.generate_dlt_rules(profiles, language="SQL")
print(dlt_expectations)

dlt_expectations = dlt_generator.generate_dlt_rules(profiles, language="Python")
print(dlt_expectations)

dlt_expectations = dlt_generator.generate_dlt_rules(profiles, language="Python_Dict")
print(dlt_expectations)

# COMMAND ----------

spark.table("stallion.learning.customers_n_test_1").show()

# COMMAND ----------

# DBTITLE 1,profiling a table
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient()
profiler = DQProfiler(ws)

summary_stats, profiles = profiler.profile_table(
    table="stallion.learning.customers_n_test_1",
    columns=["CUSTOMER_ID", "CUSTOMER_NAME", "EMAIL","SEGMENT","COUNTRY","AGE","GENDER","AGE_GROUP"],  # specify columns to profile 
    # CUSTOMER_ID,CUSTOMER_NAME,EMAIL,SEGMENT,COUNTRY,AGE,GENDER,AGE_GROUP
)

print("Summary Statistics:", summary_stats)
print("Generated Profiles:", profiles)

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
    table="stallion.learning.customers_n_test_1",
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

# dq_engine = DQEngine(WorkspaceClient())

# # Load checks from the "default" run config
# default_checks = dq_engine.load_checks_from_table(table_name="stallion.learning.checks_table1")

# # Load checks from the "workflow_001" run config
# workflow_checks = dq_engine.load_checks_from_table(table_name="stallion.learning.checks_table1", run_config_name="workflow_001")

# # Load checks from the installation (from a table defined in 'checks_table' in the run config)
# # Only works if DQX is installed in the workspace
# workflow_checks = dq_engine.load_checks_from_installation(method="table", assume_user=True, run_config_name="workflow_001")

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
    name="age_must_be_in_valid_range",
    criticality="error",
    check_func=check_funcs.is_in_range,
    column="AGE",
    check_func_kwargs={
        "min_limit": 0,
        "max_limit": 100
    },
    user_metadata={
      "check_type": "completeness",
      "responsible_data_steward": "someone@email.com"
    },
  ),

  DQRowRule(  # check with user metadata
    name="col_5_gmail",
    criticality="error",
    check_func=check_funcs.regex_match,
    column="EMAIL",
    check_func_kwargs={
        "regex": "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+"
    },
    user_metadata={
      "check_type": "completeness",
      "responsible_data_steward": "someone@email.com"
    },
  ),
    DQRowRule(  # check with user metadata
    name="col_6_gender_check",
    criticality="error",
    check_func=check_funcs.is_in_list,
    column="Gender",
    check_func_kwargs={
        "allowed": ["Female", "Male"]
    },
    user_metadata={
        "check_type": "completeness",
        "responsible_data_steward": "someone@email.com"
    },
  ),
    DQRowRule(  # check with user metadata
        name="segment_is_valid",
        criticality="warn",
        check_func=check_funcs.is_in_list,
        column="SEGMENT",
        check_func_kwargs={
            "allowed": ["Premium", "Standard", "Basic"]
        },
        user_metadata={
        "check_type": "completeness",
        "responsible_data_steward": "someone@email.com"
    },
  ),
    DQRowRule(  # check with user metadata
        name="country_should_not_be_numeric",
    criticality="error",
    check_func=check_funcs.regex_match,
    column="COUNTRY",
    check_func_kwargs={
        "regex": r"^[A-Za-z\s]+$"  # allows letters and spaces only
    },
    user_metadata={
        "check_type": "validity",
        "responsible_data_steward": "someone@email.com"
    },
  ),
    DQRowRule(
    name="AGE_GROUP_should_be_numeric_range",
    criticality="error",
    check_func=check_funcs.regex_match,
    column="AGE_GROUP",
    check_func_kwargs={
        "regex": r"^\d{2}-\d{2}$"  # Matches patterns like 20-30, 40-49, etc.
    },
    user_metadata={
        "check_type": "format_validation",
        "responsible_data_steward": "someone@email.com"
    },
),
]

# input_df = spark.read.table("stallion.learning.customers_n_test_1")
# workflow_checks = dq_engine.load_checks_from_table(table_name="stallion.learning.checks_table1", run_config_name="workflow_001")
# checks = default_checks + workflow_checks

input_df = spark.read.table("stallion.learning.customers_n_test_1")

valid_df_1, quarantine_df_1 = dq_engine.apply_checks_and_split(input_df, checks)
dq_engine.save_results_in_table(
  output_df_18=valid_df_1, 
  quarantine_df_18=quarantine_df_1, 
  output_config_18=OutputConfig("stallion.learning.customersvaliddata_28"), 
  quarantine_config_18=OutputConfig("stallion.learning.customersquarantine_data_28")
)

# Option 2: apply quality rules on the dataframe and report issues as additional columns (`_warning` and `_error`)
valid_and_quarantine_df = dq_engine.apply_checks_by_metadata(input_df, checks)
dq_engine.save_results_in_table(
  output_df_18= valid_and_quarantine_df_18, 
  output_config_18=OutputConfig("stallion.learning.customersvalid_and_quarantine_data_28")
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
    name="age_must_be_in_valid_range",
    criticality="error",
    check_func=check_funcs.is_in_range,
    column="AGE",
    check_func_kwargs={
        "min_limit": 0,
        "max_limit": 100
    },
    user_metadata={
      "check_type": "completeness",
      "responsible_data_steward": "someone@email.com"
    },
  ),

  DQRowRule(  # check with user metadata
    name="col_5_gmail",
    criticality="error",
    check_func=check_funcs.regex_match,
    column="EMAIL",
    check_func_kwargs={
        "regex": "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+"
    },
    user_metadata={
      "check_type": "completeness",
      "responsible_data_steward": "someone@email.com"
    },
  ),
    DQRowRule(  # check with user metadata
    name="col_6_gender_check",
    criticality="error",
    check_func=check_funcs.is_in_list,
    column="Gender",
    check_func_kwargs={
        "allowed": ["Female", "Male"]
    },
    user_metadata={
        "check_type": "completeness",
        "responsible_data_steward": "someone@email.com"
    },
  ),
    DQRowRule(  # check with user metadata
        name="segment_is_valid",
        criticality="warn",
        check_func=check_funcs.is_in_list,
        column="SEGMENT",
        check_func_kwargs={
            "allowed": ["Premium", "Standard", "Basic"]
        },
        user_metadata={
        "check_type": "completeness",
        "responsible_data_steward": "someone@email.com"
    },
  ),
    DQRowRule(  # check with user metadata
        name="country_should_not_be_numeric",
    criticality="error",
    check_func=check_funcs.regex_match,
    column="COUNTRY",
    check_func_kwargs={
        "regex": r"^[A-Za-z\s]+$"  # allows letters and spaces only
    },
    user_metadata={
        "check_type": "validity",
        "responsible_data_steward": "someone@email.com"
    },
  ),
    DQRowRule(
    name="AGE_GROUP_should_be_numeric_range",
    criticality="error",
    check_func=check_funcs.regex_match,
    column="AGE_GROUP",
    check_func_kwargs={
        "regex": r"^\d{2}-\d{2}$"  # Matches patterns like 20-30, 40-49, etc.
    },
    user_metadata={
        "check_type": "format_validation",
        "responsible_data_steward": "someone@email.com"
    },
),
]

input_df = spark.read.table("stallion.learning.customers_n_test_1")

# Option 1: apply quality rules on the dataframe and provide valid and invalid (quarantined) dataframes
valid_df, quarantine_df = dq_engine.apply_checks_and_split(input_df, checks)
dq_engine.save_results_in_table(
  output_df=valid_df, 
  quarantine_df=quarantine_df, 
  output_config=OutputConfig("stallion.learning.cusvaliddata_n_test_28"), 
  quarantine_config=OutputConfig("stallion.learning.cusquarantine_data_n_test_28")
)

# Option 2: apply quality rules on the dataframe and report issues as additional columns (`_warning` and `_error`)
valid_and_quarantine_df = dq_engine.apply_checks(input_df, checks)
dq_engine.save_results_in_table(
  output_df=valid_and_quarantine_df, 
  output_config=OutputConfig("stallion.learning.cusvalid_and_quarantine_data_n_test_18")
)

# # Option 1: apply quality rules on the dataframe and provide valid and invalid (quarantined) dataframes
# valid_df_metadata, quarantine_df_metadata = dq_engine.apply_checks_by_metadata_and_split(input_df, checks)
# dq_engine.save_results_in_table(
#   output_df_m=quarantine_df_metadata, 
#   quarantine_df_m=quarantine_df_metadata, 
#   output_config_m=OutputConfig("stallion.learning.customersvaliddata_test"), 
#   quarantine_config_m=OutputConfig("stallion.learning.customersquarantine_data")
# )

# # Option 2: apply quality rules on the dataframe and report issues as additional columns (`_warning` and `_error`)
# valid_and_quarantine_df = dq_engine.apply_checks_by_metadata(input_df, checks)
# dq_engine.save_results_in_table(
#   output_df= valid_and_quarantine_df, 
#   output_config=OutputConfig("stallion.learning.customersvalid_and_quarantine_data")
# )

# COMMAND ----------

# from databricks.labs.dqx import check_funcs
# from databricks.labs.dqx.config import OutputConfig
# from databricks.labs.dqx.engine import DQEngine
# from databricks.labs.dqx.rule import DQRowRule, DQDatasetRule, DQForEachColRule
# from databricks.sdk import WorkspaceClient


# dq_engine = DQEngine(WorkspaceClient())

# checks = [
# *DQForEachColRule(
#     columns=["CUSTOMER_ID", "CUSTOMER_NAME", "EMAIL", "SEGMENT", "COUNTRY", "AGE", "GENDER", "AGE_GROUP"],
#     criticality="error",
#     check_func=check_funcs.is_not_null
# ).get_rules(),

#   DQRowRule(  # check with a filter
#     name="age_must_be_in_valid_range",
#     criticality="error",
#     check_func=check_funcs.is_in_range,
#     column="AGE",
#     check_func_kwargs={
#         "min_limit": 0,
#         "max_limit": 100
#     },
#     filter="AGE > 0 AND AGE < 100",
#     user_metadata={
#       "check_type": "completeness",
#       "responsible_data_steward": "someone@email.com"
#     },
#   ),

#   DQRowRule(  # check with user metadata
#     name="col_5_gmail",
#     criticality="error",
#     check_func=check_funcs.regex_match,
#     column="EMAIL",
#     check_func_kwargs={
#         "regex": "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+"
#     },
#     user_metadata={
#       "check_type": "completeness",
#       "responsible_data_steward": "someone@email.com"
#     },
#   ),
#     DQRowRule(  # check with user metadata
#     name="col_6_gender_check",
#     criticality="error",
#     check_func=check_funcs.is_in_list,
#     column="Gender",
#     check_func_kwargs={
#         "allowed": ["Female", "Male"]
#     },
#     user_metadata={
#         "check_type": "completeness",
#         "responsible_data_steward": "someone@email.com"
#     },
#   ),
#     DQRowRule(  # check with user metadata
#         name="segment_is_valid",
#         criticality="warn",
#         check_func=check_funcs.is_in_list,
#         column="SEGMENT",
#         check_func_kwargs={
#             "allowed": ["Premium", "Standard", "Basic"]
#         },
#         user_metadata={
#         "check_type": "completeness",
#         "responsible_data_steward": "someone@email.com"
#     },
#   ),
#     DQRowRule(  # check with user metadata
#         name="country_should_not_be_numeric",
#     criticality="error",
#     check_func=check_funcs.regex_match,
#     column="COUNTRY",
#     check_func_kwargs={
#         "regex": r"^[A-Za-z\s]+$"  # allows letters and spaces only
#     },
#     user_metadata={
#         "check_type": "validity",
#         "responsible_data_steward": "someone@email.com"
#     },
#   ),
#     DQRowRule(
#     name="AGE_GROUP_should_be_numeric_range",
#     criticality="error",
#     check_func=check_funcs.regex_match,
#     column="AGE_GROUP",
#     check_func_kwargs={
#         "regex": r"^\d{2}-\d{2}$"  # Matches patterns like 20-30, 40-49, etc.
#     },
#     user_metadata={
#         "check_type": "format_validation",
#         "responsible_data_steward": "someone@email.com"
#     },
# )
# ]

# input_df = spark.read.table("stallion.learning.customers_n_test_1")

# # Option 1: apply quality rules on the dataframe and provide valid and invalid (quarantined) dataframes
# valid_df, quarantine_df = dq_engine.apply_checks_and_split(input_df, checks)
# dq_engine.save_results_in_table(
#   output_df=valid_df, 
#   quarantine_df=quarantine_df, 
#   output_config=OutputConfig("stallion.learning.cusvaliddata_n_test_1"), 
#   quarantine_config=OutputConfig("stallion.learning.cusquarantine_data_n_test_1")
# )

# # Option 2: apply quality rules on the dataframe and report issues as additional columns (`_warning` and `_error`)
# valid_and_quarantine_df = dq_engine.apply_checks(input_df, checks)
# dq_engine.save_results_in_table(
#   output_df=valid_and_quarantine_df, 
#   output_config=OutputConfig("stallion.learning.cusvalid_and_quarantine_data_n_test_1")
# )


# COMMAND ----------

display(quarantine_df)
valid_and_quarantine_df.display()
valid_df.display()

# COMMAND ----------

# DBTITLE 1,# -- Visual 1: Error vs Warning Summary
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   COUNT_IF(_errors IS NOT NULL) AS error_count,
# MAGIC   COUNT_IF(_warnings IS NOT NULL) AS warning_count
# MAGIC FROM stallion.learning.cusvalid_and_quarantine_data_n_test_18;
# MAGIC

# COMMAND ----------

##-- Visual 2: Column-wise error and warning counts
SELECT 
  column_name,
  rule_violated,
  COUNT(*) AS count
FROM stallion.learning.cusquarantine_data_n_test_28
GROUP BY column_name, rule_violated
ORDER BY count DESC;


# COMMAND ----------

display(spark.read.table("samples.nyctaxi.trips"))

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### View data quality in DQX Dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC Note: Dashboard is only using quarantined data as input. If you apply checks to annotate invalid records without quarantining them (e.g. using the `apply_checks_by_metadata` method), ensure that the `quarantine_table` field in your run config is set to the same value as the `output_table` field.

# COMMAND ----------

from databricks.labs.dqx.contexts.workspace import WorkspaceContext

ctx = WorkspaceContext(WorkspaceClient())
dashboards_folder_link = f"{ctx.installation.workspace_link('')}dashboards/"
print(f"Open a dashboard from the following folder and refresh it:")
print(dashboards_folder_link)

# COMMAND ----------

folder_path = "Workspace/Users/twitterstallion2192@gmail.com/(Clone) DQX_Test"
folder_link = ctx.installation.workspace_link(folder_path)
print(folder_link)

# COMMAND ----------

from databricks.labs.dqx.contexts.workspace import WorkspaceContext
from databricks.sdk import WorkspaceClient

ctx = WorkspaceContext(WorkspaceClient())

# Use the relative path from the workspace root
folder_path = "Workspace/Users/twitterstallion2192@gmail.com/(Clone) DQX_Test"
notebook_id = "notebooks/2836779311564559"

# Generate links
notebook_link = ctx.installation.workspace_link(notebook_id)
folder_link = ctx.installation.workspace_link(folder_path)

print("Open the notebook directly:")
print(notebook_link)

print("\nOpen the dashboard folder and refresh it:")
print(folder_link)

# COMMAND ----------

from databricks.labs.dqx.contexts.workspace import WorkspaceContext
from databricks.sdk import WorkspaceClient

ctx = WorkspaceContext(WorkspaceClient())
dashboards_folder_path = "Workspace/Users/twitterstallion2192@gmail.com/(Clone) DQX_Test/dashboards"
dashboards_folder_link = ctx.installation.workspace_link(dashboards_folder_path)
print("Open a dashboard from the following folder and refresh it:")
print(dashboards_folder_link)

# COMMAND ----------

# ctx = WorkspaceContext(WorkspaceClient())  # Ensure 'dqx' application is installed before proceeding

# dashboards_folder_path = "Workspace/Users/twitterstallion2192@gmail.com/(Clone) DQX_Test/dashboards"
# dashboards_folder_link = ctx.installation.workspace_link(dashboards_folder_path)

# print("Open a dashboard from the following folder and refresh it:")
# print(dashboards_folder_link)
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.profiler.dlt_generator import DQDltGenerator
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.contexts.workspace import WorkspaceContext
ctx = WorkspaceContext(WorkspaceClient())
dashboards_folder_link = f"{ctx.installation.workspace_link('')}dashboards/"
print(f"Open a dashboard from the following folder and refresh it:")
print(dashboards_folder_link)

# COMMAND ----------

# DBTITLE 1,End-to-end Quality Checking
# from databricks.labs.dqx import check_funcs
# from databricks.labs.dqx.config import InputConfig, OutputConfig
# from databricks.labs.dqx.engine import DQEngine
# from databricks.labs.dqx.rule import DQRowRule
# from databricks.sdk import WorkspaceClient

# dq_engine = DQEngine(WorkspaceClient())

# # Define some checks using DQRule classes
# checks = [
#  DQRowRule(
#         name="id_not_null",
#         criticality="error",
#         check_func=check_funcs.is_not_null,
#         column="CUSTOMER_ID",
#     ),
#     DQRowRule(
#         name="AGE_positive",
#         criticality="warn",
#         check_func=check_funcs.is_not_null_and_not_empty,
#         column="AGE"
#     ),
# ]

# # Option 1: Split data and write to separate output tables (validated and quarantined data)
# dq_engine.apply_checks_and_save_in_table(
#     checks=checks,
#     input_config=InputConfig("stallion.learning.customers_n_test_1"),
#     output_config=OutputConfig("stallion.learning.valid_data_n"), 
#     quarantine_config=OutputConfig("stallion.learning.quarantine_data_n")
# )

# # Option 2: Write all data with error/warning columns to a single output table
# dq_engine.apply_checks_and_save_in_table(
#     checks=checks,
#     input_config=InputConfig("stallion.learning.customers_n_test_1"),
#     output_config=OutputConfig("stallion.learning.valid_and_quarantine_data_n"),
# )

# COMMAND ----------

# %sql
# select * from stallion.learning.valid_and_quarantine_data

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
# from pyspark.sql.functions import lit, struct
# from pyspark.sql.types import StructType, StructField, StringType, MapType

# # Define the schema for the quality rules
# schema = StructType([
#     StructField("name", StringType(), False),
#     StructField("criticality", StringType(), False),
#     StructField("check", StructType([
#         StructField("function", StringType(), False),
#         StructField("arguments", MapType(StringType(), StringType()), False)
#     ]), False),
#     StructField("filter", StringType(), True),
#     StructField("run_config_name", StringType(), True),
#     StructField("user_metadata", MapType(StringType(), StringType()), True)
# ])

# # Sample data for the quality rules
# data = [
#     (
#         "is_not_null_customer_id", # Rule name 
#         "error", # Criticality
#         {"function": "is_not_null", "arguments": {"column": "CUSTOMER_ID"}}, # Check function and arguments
#         None, # Filter
#         "default", # Run config name
#         {} # User metadata
#     ),
#     (
#         "is_not_null_customer_name",
#         "error",
#         {"function": "is_not_null", "arguments": {"column": "CUSTOMER_NAME"}},
#         None,
#         "default",
#         {}
#     ),
#     (
#         "is_not_null_email",
#         "error",
#         {"function": "is_not_null", "arguments": {"column": "EMAIL"}},
#         None,
#         "default",
#         {}
#     ),
#     (
#         "is_not_null_segment",
#         "error",
#         {"function": "is_not_null", "arguments": {"column": "SEGMENT"}},
#         None,
#         "default",
#         {}
#     ),
#     (
#         "is_not_null_country",
#         "error",
#         {"function": "is_not_null", "arguments": {"column": "COUNTRY"}},
#         None,
#         "default",
#         {}
#     ),
#     (
#         "is_not_null_age",
#         "error",
#         {"function": "is_not_null", "arguments": {"column": "AGE"}},
#         None,
#         "default",
#         {}
#     ),
#     (
#         "is_not_null_gender",
#         "error",
#         {"function": "is_not_null", "arguments": {"column": "GENDER"}},
#         None,
#         "default",
#         {}
#     ),
#     (
#         "is_not_null_age_group",
#         "error",
#         {"function": "is_not_null", "arguments": {"column": "AGE_GROUP"}},
#         None,
#         "default",
#         {}
#     ),
#     (
#         "age_must_be_positive",
#         "error",
#         {"function": "is_in_range", "arguments": {"column": "AGE", "min": "0", "max": "100"}},
#         None,
#         "default",
#         {}
#     )
# ]

# rules_df = spark.createDataFrame(data, schema)

# # Write the DataFrame to a Delta table
# rules_df.write.format("delta").mode("overwrite").saveAsTable("customer_quality_rules")


# COMMAND ----------

# %sql
# select * from customer_quality_rules

# COMMAND ----------

# from databricks.labs.dqx.engine import DQEngine
# from databricks.labs.dqx.config import OutputConfig
# from databricks.sdk import WorkspaceClient

# # Initialize the DQEngine
# dq_engine = DQEngine(WorkspaceClient())

# # Load the quality rules from the Delta table
# rules_df = spark.table("customer_quality_rules")
# rules = rules_df.collect()

# # Prepare the checks list
# checks = []
# for row in rules:    from databricks.labs.dqx.engine import DQRule
# check = DQRule(
#         name=row["name"],
#         criticality=row["criticality"],
#         check=row["check"],
#         filter=row["filter"],
#         run_config_name=row["run_config_name"],
#         user_metadata=row["user_metadata"]
#     )
# checks.append(check)

# check = {
#         "name": row["name"],
#         "criticality": row["criticality"],
#         "check": row["check"].asDict(),
#         "filter": row["filter"],
#         "run_config_name": row["run_config_name"],
#         "user_metadata": row["user_metadata"]
#     }
# checks.append(check)

# # Load the customer data
# input_df = spark.read.table("stallion.learning.customers")

# # Apply the quality checks
# valid_df, quarantine_df = dq_engine.apply_checks_and_split(input_df, checks)

# # Save the results
# dq_engine.save_results_in_table(
#     output_df=valid_df,
#     quarantine_df=quarantine_df,
#     output_config=OutputConfig("stallion.learning.cusvaliddata17"),
#     quarantine_config=OutputConfig("stallion.learning.cusquarantine_data17")
# )


# COMMAND ----------

# from databricks.sdk import WorkspaceClient
# from databricks.labs.dqx import check_funcs
# from databricks.labs.dqx.config import OutputConfig
# from databricks.labs.dqx.engine import DQEngine
# from databricks.labs.dqx.rule import DQRowRule, DQForEachColRule,DQDatasetRule

# # Initialize DQEngine
# dq_engine = DQEngine(WorkspaceClient())

# # Define quality checks
# checks = [
# DQRowRule(  # check with a filter
#     name="col_4_is_null_or_empty",
#     criticality="warn",
#     filter="AGE < 0",
#     check_func=check_funcs.is_not_null_and_not_empty,
#     column="AGE",
#   ),

#   DQRowRule(  # check with user metadata
#     name="col_5_is_null_or_empty",
#     criticality="warn",
#     check_func=check_funcs.is_not_null_and_not_empty,
#     column="EMAIL",
#     user_metadata={
#       "check_type": "completeness",
#       "responsible_data_steward": "someone@email.com"
#     },
#   ),
#     *DQForEachColRule(
#         columns=["CUSTOMER_ID", "CUSTOMER_NAME", "EMAIL", "SEGMENT", "COUNTRY", "AGE", "GENDER", "AGE_GROUP"],
#         criticality="error",
#         check_func=check_funcs.is_not_null
#     ).get_rules()
# ]
# additional_checks = [
#     DQDatasetRule(
#         name="customer_id_unique",
#         criticality="error",
#         check_func=check_funcs.is_unique,
#         columns=["CUSTOMER_ID"]
#     ),
#     DQRowRule(
#         name="age_group_consistency",
#         criticality="error",
#         check_func=check_funcs.regex_match,
#         column="AGE_GROUP",
#         regex=r"^\d{1,2}-\d{1,2}$"
#     ),
#     DQRowRule(
#         name="gender_validity",
#         criticality="error",
#         check_func=check_funcs.value_is_in_list,
#         column="GENDER",
#         allowed=["M", "F", "Other"]
#     ),
#     DQRowRule(
#         name="email_domain_check",
#         criticality="warn",
#         check_func=check_funcs.regex_match,
#         column="EMAIL",
#         regex=r"@company\.com$"
#     ),
#     DQRowRule(
#         name="country_validity",
#         criticality="error",
#         check_func=check_funcs.value_is_in_list,
#         column="COUNTRY",
#         allowed=["India", "USA", "UK", "Canada"]
#     ),
#     DQRowRule(
#         name="age_group_range_check",
#         criticality="error",
#         check_func=check_funcs.is_in_range,
#         column="AGE",
#         min_limit=18,
#         max_limit=25
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
# all_checks = checks + additional_checks


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

# valid_df, quarantine_df = dq_engine.apply_checks_and_split(input_df, all_checks)
# dq_engine.save_results_in_table(
#     output_df=valid_df,
#     quarantine_df=quarantine_df,
#     output_config=OutputConfig("stallion.learning.cusvaliddata1"),
#     quarantine_config=OutputConfig("stallion.learning.cusquarantine_data1")
# )

# COMMAND ----------

# # Load customer data
# input_df = spark.read.table("stallion.learning.customers")

# # Apply quality checks and split data into valid and quarantined datasets
# valid_df, quarantine_df = dq_engine.apply_checks_and_split(input_df, checks)

# # Save results to Delta tables
# dq_engine.save_results_in_table(
#     output_df=valid_df,
#     quarantine_df=quarantine_df,
#     output_config=OutputConfig("stallion.learning.cusvaliddata"),
#     quarantine_config=OutputConfig("stallion.learning.cusquarantine_data")
# )


# COMMAND ----------

# from pyspark.sql.types import StructType, StructField, StringType, MapType

# # Define schema for quality rules
# schema = StructType([
#     StructField("name", StringType(), nullable=False),
#     StructField("criticality", StringType(), nullable=False),
#     StructField("check", StructType([
#         StructField("function", StringType(), nullable=False),
#         StructField("arguments", MapType(StringType(), StringType()), nullable=True)
#     ]), nullable=False),
#     StructField("filter", StringType(), nullable=True),
#     StructField("run_config_name", StringType(), nullable=True),
#     StructField("user_metadata", MapType(StringType(), StringType()), nullable=True)
# ])

# # Example data for quality rules
# data = [
#     ("is_not_null_customer_id", "error", {"function": "is_not_null", "arguments": {"column": "CUSTOMER_ID"}}, None, "default", None),
#     ("age_must_be_positive", "error", {"function": "is_in_range", "arguments": {"column": "AGE", "min_limit": "0", "max_limit": "100"}}, None, "default", None)
# ]

# # Create DataFrame for quality rules
# rules_df = spark.createDataFrame(data, schema)

# # Save to Delta table
# rules_df.write.format("delta").mode("overwrite").saveAsTable("stallion.learning.customer_quality_rules1")


# COMMAND ----------

# # Load quality rules from Delta table
# rules_df = spark.read.table("stallion.learning.customer_quality_rules")

# # Convert rules to DQX format (this step depends on your implementation)
# # For example, you might need to transform the rules_df into a list of DQRowRule or DQForEachColRule objects

# # Apply quality checks to customer data
# valid_df, quarantine_df = dq_engine.apply_checks_and_split(input_df, checks)

# # Save results
# dq_engine.save_results_in_table(
#     output_df=valid_df,
#     quarantine_df=quarantine_df,
#     output_config=OutputConfig("stallion.learning.cusvaliddata"),
#     quarantine_config=OutputConfig("stallion.learning.cusquarantine_data")
# )


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


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = SparkSession.builder.getOrCreate()

# Define schema
schema = StructType([
    StructField("patient_id", IntegerType(), True),
    StructField("test_name", StringType(), True),
    StructField("result_value", DoubleType(), True),
    StructField("result_date", StringType(), True)
])

# Modified schema for EMR with result_value as StringType
emr_schema_mismatch = StructType([
    StructField("patient_id", IntegerType(), True),
    StructField("test_name", StringType(), True),
    StructField("result_value", StringType(), True),  # <-- Mismatched Type
    StructField("result_date", StringType(), True)
])

# Data with a string result_value
emr_data_mismatch = [(201, "HbA1c", "5.7", "2025-07-19"), (202, "Cholesterol", "190", "2025-07-20")]

# Sample data
lab_data = [(101, "HbA1c", 5.6, "2025-07-20"), (102, "Cholesterol", 180.0, "2025-07-21")]
# emr_data = [(201, "HbA1c", 5.7, "2025-07-19"), (202, "Cholesterol", 190.0, "2025-07-20")]

# Create DataFrame with mismatched schema
emr_df_mismatch = spark.createDataFrame(emr_data_mismatch, schema=emr_schema_mismatch)

# Create DataFrames
lab_df = spark.createDataFrame(lab_data, schema=schema)
# emr_df = spark.createDataFrame(emr_data, schema=schema)

# COMMAND ----------

def compare_schemas(df1, df2):
    fields1 = [(f.name, f.dataType) for f in df1.schema.fields]
    fields2 = [(f.name, f.dataType) for f in df2.schema.fields]
    return fields1 == fields2

schema_match = compare_schemas(lab_df, emr_df_mismatch)
print("Schema Match:", schema_match)

# Show mismatched fields if schema doesn't match
def schema_differences(df1, df2):
    d1 = {f.name: f.dataType for f in df1.schema.fields}
    d2 = {f.name: f.dataType for f in df2.schema.fields}
    mismatch = {k: (d1.get(k), d2.get(k)) for k in set(d1) | set(d2) if d1.get(k) != d2.get(k)}
    return mismatch

if not schema_match:
    diffs = schema_differences(lab_df, emr_df_mismatch)
    print("Differences in schema:")
    for col, (type1, type2) in diffs.items():
        print(f"Column: {col} | Dataset1: {type1} | Dataset2: {type2}")

# COMMAND ----------

def twitter(a,b):
    c=a+b
    return c
def stwitter(*a):
    for i in a:
        print(i)
    return 

# COMMAND ----------

print(twitter([2,3]))

# COMMAND ----------

# %pip install nbformat

# COMMAND ----------

from pathlib import Path
from nbformat import v4 as nbf



# COMMAND ----------

from pathlib import Path
from nbformat import v4 as nbf

# Create a new notebook
nb = nbf.new_notebook()

# Define the cells
cells = [

    nbf.new_markdown_cell("# 📊 Data Quality Monitoring Dashboard\nThis notebook demonstrates how to build visualizations in Databricks for monitoring data quality using sample data."),
    
    nbf.new_code_cell(
        '''# ✅ Create sample data
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

# Sample order_amount data
order_data = [
    (1, 100.0, '2025-07-21', 'error', 'yellow'),
    (2, 200.0, '2025-07-21', 'warning', 'warning'),
    (3, 150.0, '2025-07-22', None, None),
    (4, 250.0, '2025-07-22', 'error', 'yellow'),
    (5, 300.0, '2025-07-23', 'warning', 'warning'),
    (6, 120.0, '2025-07-23', None, None)
]

columns = ["order_id", "amount", "load_date", "data_quality_flag", "rule_violated"]

df_orders = spark.createDataFrame(order_data, columns)
df_orders.createOrReplaceTempView("order_amount")
df_orders.show()
'''
    ),

    nbf.new_code_cell(
        '''# ✅ Create sample violation data
violation_data = [
    ("amount", "yellow"),
    ("amount", "warning"),
    ("order_id", "yellow"),
    ("amount", "warning")
]

df_violations = spark.createDataFrame(violation_data, ["column_name", "rule_violated"])
df_violations.createOrReplaceTempView("order_amount_violations")
df_violations.show()
'''
    ),

    nbf.new_code_cell(
        '''%sql
-- Sample Error Count
SELECT COUNT(*) AS error_count
FROM order_amount
WHERE data_quality_flag = 'error';'''
    ),

    nbf.new_code_cell(
        '''%sql
-- Sample Warning Count
SELECT COUNT(*) AS warning_count
FROM order_amount
WHERE data_quality_flag = 'warning';'''
    ),

    nbf.new_code_cell(
        '''%sql
-- Sample Yellow Rule Violation Percentage
SELECT 
  ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM order_amount)), 2) AS yellow_rule_violation_percent
FROM order_amount
WHERE rule_violated = 'yellow';'''
    ),

    nbf.new_code_cell(
        '''%sql
-- Sample Warning Rule Violation Percentage
SELECT 
  ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM order_amount)), 2) AS warning_rule_violation_percent
FROM order_amount
WHERE rule_violated = 'warning';'''
    ),

    nbf.new_code_cell(
        '''%sql
-- Error/Warning Count by Load Date
SELECT load_date, COUNT(*) AS count, data_quality_flag
FROM order_amount
GROUP BY load_date, data_quality_flag
ORDER BY load_date;'''
    ),

    nbf.new_code_cell(
        '''%sql
-- Column-level Violations
SELECT column_name, COUNT(*) AS count, rule_violated
FROM order_amount_violations
GROUP BY column_name, rule_violated;'''
    ),

    nbf.new_markdown_cell(
        "👉 **Instructions to Build Dashboard:**\n"
        "1. Run each SQL cell.\n"
        "2. Click on the \"+\" under each result to create a visualization.\n"
        "3. Pin the visualizations to a new dashboard called `Monitor Data Quality`.\n"
        "4. Open the dashboard and arrange your widgets.\n"
        "5. Optionally, set auto-refresh or sharing options."
    )
]

# Add cells to notebook
nb['cells'] = cells

# Save notebook to a file
output_path = Path("Databricks_Data_Quality_Dashboard.ipynb")
with output_path.open("w", encoding="utf-8") as f:
    f.write(nbf.writes(nb))

output_path.name


# COMMAND ----------

from pyspark.sql.functions import * 
from pyspark.sql.window import Window
data = [
 ("C001", "2024-01-01"),
 ("C001", "2024-01-02"),
 ("C001", "2024-01-04"),
 ("C001", "2024-01-06"),
 ("C002", "2024-01-03"),
 ("C002", "2024-01-05"),
]

df = spark.createDataFrame(data, ["customer_id", "billing_date"])
df1=df.withColumn("pr", lag("billing_date",1).over(Window.partitionBy("customer_id").orderBy("billing_date")))
df2=df1.withColumn("diff", datediff(col("billing_date"),col("pr")))
df3=df2.filter(col("diff")>1)
df3.display()
result_df=df3.withColumn("missing_from",date_add(col("pr"),1))\
					.withColumn("missing_to",date_sub(col('billing_date'),1)).display()

# COMMAND ----------

sen1 = "Geeks for Geeks fun"
sen2 = "Learning from Geeks for Geeks"

# Split sentences into words
words1 = set(sen1.split())
words2 = set(sen2.split())

n=len(words1.intersection(words2))
print(n)
n1=list((words1)-(words2) | (words2)-(words1))
print(n1)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
#window based function sytnax
df1=df.withColumn("n", row_number().over(Window.partitionBy("c1").orderBy("c2")))\
    .withColumn("rn", rank().over(Window.partitionBy("c1").orderBy("c2")))\
        .withColumn("den", dense_rank().over(Window.partitionBy("department").orderBy(desc("salary"))))\
            .filter(col("den")==2)\
            .withColumn("sum1", sum("amount").over(Window.partitionBy("id").orderBy(col("amount").desc()).rowsBetween(Window.unboundedPreceding, Window.currentRow)))\
                .withColumn("avg", avg("amount").over(Window.partitionBy("id").orderBy(col("amount").desc()).rowsBetween(Window.currentRow, Window.unboundedFollowing)))


# COMMAND ----------

data = [
 ("12345", '{"id": 12345, "name": "Jane Doe", "email": "jane.doe@example.com"□""}'),
 ("12346", '{"id": 12346, "name": "John Smith", "email": "john.smith@example.com"✅"}')
]

# Define schema for the DataFrame
columns = ["id", "json_data"]
from pyspark.sql.functions import *
from pyspark.sql.types import *
# Create DataFrame
df = spark.createDataFrame(data, columns)
# df.display()
df1=df.withColumn("js",  regexp_replace("json_data", '□"', ""))\
        .withColumn("js", regexp_replace("js", "✅",""))\
        .withColumn("js", regexp_replace("js", '""','"'))
json_schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True)
])

# Parse the cleaned JSON data
df_parsed = df1.withColumn("parsed_json", from_json(col("js"), json_schema))
df_final = df_parsed.select("parsed_json.*").show()

# COMMAND ----------

df_name = spark.createDataFrame([("sai praneeth",)], ["name"])
df1=df_name.withColumn("name1", initcap("name")).display()
i='jeswanth thulasiraman'
print(i.title())
#second k='madam'
#palendrome
print(k==k[::-1])

# COMMAND ----------

Why is caching so important in PySpark? When’s the best time to use it?
--> caching is  use to store the intermediate data in memory to avoid recomputation and improve performance.Its best for the small data which can be stored in the memory according to the cluster configuration 
2. How do cache() and persist() differ? When would you pick one over the other?
-->cache is in memory and persist is to store both in memory and disk as well. 
cache is for small datasets and persist is for large datasets
3. What makes Kryo serialization faster than the default one in PySpark?
serialization meaning converting object into byte stream. 
by default spark uses java serialization which is slow and kryo serialization is faster because it uses reflection to serialize the object and it is faster. uses byte-level encoding and avoids reflection overhead by registering classes ahead of time (if configured)
4. Why do formats like Parquet and ORC make jobs faster? What’s their advantage?
columnar format storage and compression.
5. When should you use coalesce() vs. repartition()? Can you share an example?
6. What does spark.sql.shuffle.partitions do, and why adjust it?
7. What happens if your data is skewed? How can you deal with it in PySpark?
8. How are wide and narrow transformations different? Why are wide ones slower?
9. What’s predicate pushdown, and how does it improve performance?
10. What’s dynamic partition pruning, and how does it help speed up queries?


# COMMAND ----------

# DBTITLE 1,scd type1
source_df
history_df
#scd type1 pyspark code
soure_df.merge(history_df, "id", "leftanti")
from pyspark.sql.functions import *
from pyspark.sql.window import Window


# COMMAND ----------

data=[('Alice','Badminton,Tennis'),('Bob','Tennis,Cricket'),('Julie','Cricket,Carroms')]
columns=["Name","Hobbies"]
df=spark.createDataFrame(data,columns)
from pyspark.sql.functions import *
from pyspark.sql.types import *
df1=df.withColumn("Hobbies1", explode(split("Hobbies", ",")))
df1.show()

# COMMAND ----------

data= [('Chandrali', 28,'Sing-Dance-Play'),('Neetesh',25,'Basketball-Volleyball-Dance')]
df=spark.createDataFrame(data, schema=["name","age","hobbies"])
from pyspark.sql.functions import *
df2=df.withColumn("me", explode(split("hobbies", "-"))).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history stallion.learning.customer

# COMMAND ----------

# MAGIC %sql
# MAGIC create table stallion.learning.transactions (
# MAGIC   user_id STRING,
# MAGIC   product_id STRING,
# MAGIC   region STRING,
# MAGIC   transaction_date DATE,
# MAGIC   amount DOUBLE
# MAGIC )
# MAGIC -- #WHERE region = 'US' AND transaction_date = '2025-07-30'
# MAGIC
# MAGIC -- WHERE user_id = 'U123456'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO stallion.learning.transactions VALUES
# MAGIC   ('U1001', 'P5001', 'US', DATE('2025-07-30'), 150.00),
# MAGIC   ('U1002', 'P5002', 'US', DATE('2025-07-30'), 200.00),
# MAGIC   ('U1003', 'P5003', 'UK', DATE('2025-07-30'), 175.00),
# MAGIC   ('U1004', 'P5004', 'IN', DATE('2025-07-29'), 300.00),
# MAGIC   ('U1005', 'P5005', 'US', DATE('2025-07-28'), 125.00);
# MAGIC

# COMMAND ----------

# DBTITLE 1,frequently filtering columns
# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM stallion.learning.transactions
# MAGIC WHERE region = 'US' AND transaction_date = '2025-07-30';

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE stallion.learning.transactions
# MAGIC ZORDER BY (region, transaction_date);
# MAGIC #at the time of table creation we can do this 
# MAGIC ALTER TABLE stallion.learning.transactions
# MAGIC SET TBLPROPERTIES (
# MAGIC   'delta.liquidClustering.enabled' = 'true',
# MAGIC   'delta.liquidClustering.columns' = 'region, transaction_date'
# MAGIC );
# MAGIC
# MAGIC -- #After a successful OPTIMIZE ZORDER, you will see:
# MAGIC -- numFilesAdded > 0
# MAGIC -- numFilesRemoved > 0
# MAGIC
# MAGIC -- "zOrderStats.numOutputCubes" > 0
# MAGIC
# MAGIC -- "filesAdded.totalSize" increases (data rewritten)
# MAGIC
# MAGIC -- -- "partitionsOptimized" > 0 (if partitioned table)You Want To	Do This
# MAGIC -- Confirm Z-order ran	Look for filesAdded, zOrderStats.numOutputCubes
# MAGIC -- Make it work on small data	Lower threshold using SET spark.databricks.delta.optimize.minFileSize
# MAGIC -- -- Confirm columns were optimized	Run a query using a Z-ordered column and use the Spark UI to check input files read
# MAGIC -- spark.conf.set("spark.databricks.delta.optimize.minFileSize", "1048576")  # 1 MB
# MAGIC -- spark.conf.set("spark.databricks.delta.autoCompact.maxFileSize", "134217728")  # 128 MB
# MAGIC -- spark.conf.set("spark.databricks.delta.optimize.maxFileSize", "134217728")  # 128 MB
# MAGIC --spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")  --Combine with small file thresholds to trigger merges efficiently
# MAGIC Databricks uses several configs to manage file sizes: | Config Name | Purpose | |-------------|---------| 
# MAGIC spark.databricks.delta.optimize.minFileSize | Minimum file count to trigger compaction | | 
# MAGIC spark.databricks.delta.autoCompact.maxFileSize | Target size for compacted files | | 
# MAGIC spark.databricks.delta.optimize.maxFileSize | Controls file size during OPTIMIZE | |
# MAGIC delta.targetFileSize | Table-level property for file size tuning 

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.optimize.minFileSize = 1000;
# MAGIC

# COMMAND ----------

import pandas as pd

# Sample data
data = {
    "EmployeeID": [101, 102, 103, 104],
    "CompanyEmailID": [
        "john.doe@kpmg.com",
        "jane.smith@tcs.com",
        "sai.nandyala@tiger.com",
        "alex.brown@cognizant.com"
    ]
}

# Create DataFrame
# Remove import and SparkSession creation; Spark session is already available in Databricks
df = spark.createDataFrame(pd.DataFrame(data))

from pyspark.sql.functions import *

df1=df.withColumn("employee_firstName", regexp_extract(col("CompanyEmailID"), r"(\w+)\.", 1))\
    .withColumn("emp_secondName", regexp_extract(col("CompanyEmailID"), r"\.(\w+)\@", 1))\
        .withColumn("f", regexp_extract(col("CompanyEmailID"), r"(\w+\.\w+)", 1))\
        .withColumn("totalnam", concat(col("employee_firstName"), lit(" "), col("emp_secondName")))\
            .withColumn("companyname", regexp_extract(col("CompanyEmailID"), r"@([\w]+)", 1))\
                 .withColumn("companyname1", regexp_extract(col("CompanyEmailID"), r"@(.+)$", 1)).display()

# COMMAND ----------

