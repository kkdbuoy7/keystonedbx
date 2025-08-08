# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import DeltaTable

# COMMAND ----------

data = [(1, "Alice", "HR", 60000),
        (2, "Bob", "IT", 70000),
        (3, "Charlie", "HR", 80000),
        (4, "David", "IT", 90000)]
columns = ["emp_id", "name", "dept", "salary"]
df = spark.createDataFrame(data,columns)

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("employee_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_managed

# COMMAND ----------

new_data = [(5, "Eve", "HR", 100000),
            (6, "Frank", "IT", 90000)]
new_df = spark.createDataFrame(new_data,columns)
new_df.write.format("delta").mode("append").saveAsTable("employee_managed")

display(spark.sql("select * from employee_managed"))

# COMMAND ----------

# MAGIC %sql
# MAGIC update employee_managed set salary = 100000 where name = "Frank";
# MAGIC select * from employee_managed

# COMMAND ----------

df22 = DeltaTable.forName(spark,"employee_managed")
df22.update(
    condition = "name = 'David'",
    set = {"salary":"85000"}
)
display(spark.sql("select * from employee_managed"))

# COMMAND ----------

