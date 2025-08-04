# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Data Quality Monitoring Dashboard
# MAGIC This notebook demonstrates how to build visualizations in Databricks for monitoring data quality using sample data.

# COMMAND ----------

# ✅ Create sample data
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


# COMMAND ----------

# ✅ Create sample violation data
violation_data = [
    ("amount", "yellow"),
    ("amount", "warning"),
    ("order_id", "yellow"),
    ("amount", "warning")
]

df_violations = spark.createDataFrame(violation_data, ["column_name", "rule_violated"])
df_violations.createOrReplaceTempView("order_amount_violations")
df_violations.show()


# COMMAND ----------

# import matplotlib.pyplot as plt

# # Aggregate violation counts by rule_violated
# violation_counts = (
#     df_violations.groupBy("rule_violated")
#     .count()
#     .toPandas()
# )

# # Plot
# plt.figure(figsize=(6,4))
# plt.bar(violation_counts["rule_violated"], violation_counts["count"], color=["yellow", "orange"])
# plt.xlabel("Rule Violated")
# plt.ylabel("Count")
# plt.title("Violation Counts by Rule")
# plt.tight_layout()
# plt.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample Error Count
# MAGIC SELECT COUNT(*) AS error_count
# MAGIC FROM order_amount
# MAGIC WHERE data_quality_flag = 'error';

# COMMAND ----------

    

# COMMAND ----------

# import matplotlib.pyplot as plt

# violation_counts = df_violations.groupBy("rule_violated").count().toPandas()

# plt.figure(figsize=(6,4))
# plt.bar(violation_counts["rule_violated"], violation_counts["count"], color=["yellow", "orange"])
# plt.xlabel("Rule Violated")
# plt.ylabel("Count")
# plt.title("Violation Counts by Rule")
# plt.tight_layout()
# plt.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample Warning Count
# MAGIC SELECT COUNT(*) AS warning_count
# MAGIC FROM order_amount
# MAGIC WHERE data_quality_flag = 'warning';

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample Yellow Rule Violation Percentage
# MAGIC SELECT 
# MAGIC   ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM order_amount)), 2) AS yellow_rule_violation_percent
# MAGIC FROM order_amount
# MAGIC WHERE rule_violated = 'yellow';

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample Warning Rule Violation Percentage
# MAGIC SELECT 
# MAGIC   ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM order_amount)), 2) AS warning_rule_violation_percent
# MAGIC FROM order_amount
# MAGIC WHERE rule_violated = 'warning';

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC -- Error/Warning Count by Load Date
# MAGIC SELECT load_date, COUNT(*) AS count, data_quality_flag
# MAGIC FROM order_amount
# MAGIC GROUP BY load_date, data_quality_flag
# MAGIC ORDER BY load_date;

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC -- Column-level Violations
# MAGIC SELECT column_name, COUNT(*) AS count, rule_violated
# MAGIC FROM order_amount_violations
# MAGIC GROUP BY column_name, rule_violated;

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC 👉 **Instructions to Build Dashboard:**
# MAGIC 1. Run each SQL cell.
# MAGIC 2. Click on the "+" under each result to create a visualization.
# MAGIC 3. Pin the visualizations to a new dashboard called `Monitor Data Quality`.
# MAGIC 4. Open the dashboard and arrange your widgets.
# MAGIC 5. Optionally, set auto-refresh or sharing options.

# COMMAND ----------

