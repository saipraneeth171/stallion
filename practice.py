# Databricks notebook source
from pyspark.sql import DataFrame

# COMMAND ----------

df = spark.createDataFrame([('a',1),('b',2)])

# COMMAND ----------

class Employee:
    def __init__(this, name, age,sex):
        this.name = name
        this.age = age
        this.sex = sex
    def __repr__(this):
        return f"""Employee(
            Name :{this.name}, 
            age : {this.age}, 
            sex : {this.sex}
            )"""

# COMMAND ----------

e = Employee('saip','30','M')

# COMMAND ----------

e

# COMMAND ----------

class x:
    def __init__(self, x):
        self.a = x
    def __repr__(self):
        return f"x({self.a})"
    def print(self):
        print('(((((((())))))))',self.a)
    def __sub__(self, other):
        return x(self.a + other.a)

# COMMAND ----------

y = x(5)
t = x(10)

# COMMAND ----------

y , t

# COMMAND ----------

x = 4

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
data=[(171,"praneeth"),
      (None,"new"),
      (None,"five"),
    (123,"things"),
    (None,"ACC"),
    (None,"LTI")]
schema=["id", "Category"]

df=spark.createDataFrame(data,schema)
df1=df.withColumn("rn", row_number().over(Window.orderBy(lit(None))))\
    .withColumn("cn", count("id").over(Window.orderBy(col("rn"))))\
        .withColumn("id", first_value("id").over(Window.partitionBy("cn").orderBy(col("rn")))).display()


# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

data1 = [(1, "Alice", 23)]
data2 = [("Bob", 30, "NY")]

df1 = spark.createDataFrame(data1, ["id", "name", "age"])
df2 = spark.createDataFrame(data2, schema=["name", "age", "city"])

# union by matching column names
df_union = df1.unionByName(df2, allowMissingColumns=True)
df_union.show()


# COMMAND ----------



# COMMAND ----------

