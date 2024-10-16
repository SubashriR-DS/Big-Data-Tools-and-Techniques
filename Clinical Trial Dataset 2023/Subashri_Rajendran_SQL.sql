-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.fs.ls('/FileStore/tables/')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinicaltrial_2023 = "clinicaltrial_2023"
-- MAGIC pharma = "pharma"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import *
-- MAGIC from pyspark.sql.functions import *
-- MAGIC
-- MAGIC delimiter = {                  # dictionary in delimiters
-- MAGIC     "clinicaltrial_2023": "\t",
-- MAGIC     "clinicaltrial_2021": "|",
-- MAGIC     "clinicaltrial_2020": "|",
-- MAGIC     "pharma": ","
-- MAGIC }
-- MAGIC
-- MAGIC
-- MAGIC def create_dataframe(ct):
-- MAGIC     if ct == "clinicaltrial_2023":
-- MAGIC         rdd = sc.textFile(f"/FileStore/tables/{ct}.csv").map(lambda x: x.rstrip(',').strip('"')).map(lambda row: row.split(delimiter[ct]))
-- MAGIC         head = rdd.first()
-- MAGIC         rdd = rdd.map(lambda row: row + [" " for i in range(len(head) - len(row))] if len(row) < len(head) else row )
-- MAGIC         df = rdd.toDF()
-- MAGIC         first = df.first()
-- MAGIC         for col in range(0, len(list(first))):
-- MAGIC            df = df.withColumnRenamed(f"_{col + 1}", list(first)[col])
-- MAGIC         df = df.withColumn('index', monotonically_increasing_id())
-- MAGIC         return df.filter(~df.index.isin([0])).drop('index')
-- MAGIC     else:
-- MAGIC         return spark.read.csv(f"/FileStore/tables/{ct}.csv", sep=delimiter[ct], header = True)
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC clinicaltrial2023DF = create_dataframe(clinicaltrial_2023)
-- MAGIC pharmaFile = create_dataframe(pharma)
-- MAGIC clinicaltrial2023DF.show(5)
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC #to create a temporary table (view)
-- MAGIC clinicaltrial2023DF.createOrReplaceTempView(clinicaltrial_2023)
-- MAGIC pharmaFile.createOrReplaceTempView(pharma)

-- COMMAND ----------

SELECT * 
FROM clinicaltrial_2023

-- COMMAND ----------

SELECT *
FROM pharma                                                                                                                                                                                                           

-- COMMAND ----------

--Q1.
SELECT DISTINCT count(*) 
FROM clinicaltrial_2023

-- COMMAND ----------

--Q2.
SELECT clinicaltrial_2023.Type, count(*) as Frequency
FROM clinicaltrial_2023
GROUP BY clinicaltrial_2023.Type
ORDER BY Frequency DESC
LIMIT 3

-- COMMAND ----------

--Q3.
CREATE OR REPLACE TEMP VIEW all_conditions AS 
SELECT explode(split(clinicaltrial_2023.conditions, "\\|"))as conditions 
FROM clinicaltrial_2023;


-- COMMAND ----------

--Q3.1
SELECT conditions, count(*) as count 
FROM all_conditions
GROUP BY conditions
ORDER BY count DESC
LIMIT 5

-- COMMAND ----------

--Q4
CREATE OR REPLACE TEMP VIEW not_pharma_sponsor AS 
SELECT Sponsor 
FROM clinicaltrial_2023
WHERE Sponsor NOT IN (SELECT Parent_Company FROM pharma)


-- COMMAND ----------

--Q 4.1
SELECT Sponsor, count(*) as count FROM not_pharma_sponsor
GROUP BY Sponsor
ORDER BY count DESC
LIMIT 12

-- COMMAND ----------

--Q5.

CREATE OR REPLACE TEMP VIEW completedstudies AS 
SELECT 
    SPLIT_PART(Completion, '-', 2) AS Month,
    COUNT(*) AS Count
FROM 
    clinicaltrial_2023
WHERE 
    clinicaltrial_2023.Status IN ('COMPLETED', 'completed') 
    AND SPLIT_PART(clinicaltrial_2023.Completion, '-', 1) = '2023'
GROUP BY 
    Month
ORDER BY 
    Month;

SELECT 
    CASE Month
        WHEN '01' THEN 'January'
        WHEN '02' THEN 'February'
        WHEN '03' THEN 'March'
        WHEN '04' THEN 'April'
        WHEN '05' THEN 'May'
        WHEN '06' THEN 'June'
        WHEN '07' THEN 'July'
        WHEN '08' THEN 'August'
        WHEN '09' THEN 'September'
        WHEN '10' THEN 'October'
        WHEN '11' THEN 'November'
        WHEN '12' THEN 'December'
        ELSE 'Unknown'
    END AS Month, 
    Count 
FROM 
    completedstudies;





-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC
-- MAGIC # Data
-- MAGIC months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
-- MAGIC counts = [1494, 1272, 1552, 1324, 1415, 1619, 1360, 1230, 1152, 1058, 909, 1082]
-- MAGIC
-- MAGIC # Plot
-- MAGIC plt.figure(figsize=(10, 6))
-- MAGIC plt.bar(months, counts, color='green')
-- MAGIC plt.title('Clinical Trial Completion Count by Month in 2023')
-- MAGIC plt.xlabel('Month')
-- MAGIC plt.ylabel('Count')
-- MAGIC plt.xticks(rotation=45)
-- MAGIC plt.tight_layout()
-- MAGIC
-- MAGIC # Show plot
-- MAGIC plt.show()
-- MAGIC

-- COMMAND ----------

show tables

-- COMMAND ----------

show databases
