# Databricks notebook source
spark

# COMMAND ----------

path = r"/FileStore/customers_1000000.csv"
df = spark.read.csv(path, header = True, inferSchema = True)
df.display()

# COMMAND ----------

# check no of records in a dataframe.
df.count()

# COMMAND ----------

# print only column names from dataframe
df.columns

# COMMAND ----------

# Print datatypes of colmn in df
df.dtypes

# COMMAND ----------

# Iterate Column names

for i in df.columns:
    print(i)

# COMMAND ----------

# Print count of number of colums..
column_count = 0
for i in df.columns:
   column_count +=1
print(f"Number of columns available in dataframe:",column_count)

# COMMAND ----------

# select specific columns from dataframe

df1 = df.select("First Name","Customer Id")
df1.display()

# COMMAND ----------

# Rename column from dataframe  '\' continue code in next line
df2 = df.withColumnRenamed("Customer Id", "Customer_Id")\
        .withColumnRenamed("First Name", "First_Name")\
        .withColumnRenamed("Last Name", "Last_Name")
df2.display()

# COMMAND ----------

# How to add new column in dataframe with constant value.
# lit = It is function which adds constant value in all rows of column
# import * - import all functions available in library.
from pyspark.sql.functions import *
df3 = df.withColumn("Language", lit("English"))
df3.display()

# COMMAND ----------

# Add one column with random number in it.
from pyspark.sql.functions import monotonically_increasing_id
df4 = df.withColumn("New_Id", monotonically_increasing_id())
df4.display()

# COMMAND ----------

# Add new column in dataframe based on availble column

df5 =df4.withColumn("New_Index",df4.Index+100)
df5.display()

# COMMAND ----------

# Drop dupliacte records in specific column..
print("Count before remove duplicate values are:-",df5.count())
df6 = df5.dropDuplicates(["First Name"])
print("Count After remove duplicate values are:-", df6.count())

# COMMAND ----------

# Drop duplicate records from all columns..
print("Count before remove duplicate values are:-",df5.count())
df7 = df5.distinct()
print("Count After remove duplicate values are:-",df7.count())

# COMMAND ----------

df8 = df5.drop("New_Id")\
         .drop("New_Index")
df8.display()

# COMMAND ----------

df9 = df5.withColumnRenamed("Customer Id", "Customer_Id")\
        .withColumnRenamed("First Name", "First_Name")\
        .withColumnRenamed("Last Name", "Last_Name")


# COMMAND ----------

# Filter in PySpark
df10 = df9.filter(df9.First_Name=="Susan")
df10.display()

# COMMAND ----------

# Like Operation in PySpark

df11 = df9.filter(df9.First_Name.like("%s%"))
df11.display()

# COMMAND ----------

# WHERE Condition
df12 = df9.where(df9.Index == 9999)
df12.display()

# COMMAND ----------

# Where with & Condition
df13 = df9.where((df9.Index >= 2700) & (df9.Index <= 2720))
df13.display()

# COMMAND ----------

# orderby descending order

df14 = df9.orderBy(col("Subscription Date").desc())
df14.display()

# COMMAND ----------

# orderby ascending order

df14 = df9.orderBy(col("Subscription Date"))
df14.display()

# COMMAND ----------