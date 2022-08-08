# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC This notebook imports data from the Azure SQL server holding cost centre info and Assemble teams data
# MAGIC 
# MAGIC This notebook uses scala and python. However, the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` magic command. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 1: Connection Information
# MAGIC 
# MAGIC The connection uses Scala
# MAGIC 
# MAGIC Password and Username for Azure SQL are stored in a databricks scope - created via the databricks CLI tool on my local machine. The scope is called finance-sql.

# COMMAND ----------

# MAGIC %scala
# MAGIC Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
# MAGIC val jdbcHostname = "brc-uks-financelogicalsqlserver-test.database.windows.net"
# MAGIC val jdbcPort = 1433
# MAGIC val jdbcDatabase = "brc-uks-finance-test"
# MAGIC 
# MAGIC // Create the JDBC URL without passing in the user and password parameters.
# MAGIC val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"
# MAGIC 
# MAGIC // Create a Properties() object to hold the parameters.
# MAGIC import java.util.Properties
# MAGIC val connectionProperties = new Properties()
# MAGIC 
# MAGIC val jdbcUsername = dbutils.secrets.get(scope = "finance-sql", key = "username")
# MAGIC val jdbcPassword = dbutils.secrets.get(scope = "finance-sql", key = "password")
# MAGIC connectionProperties.put("user", s"${jdbcUsername}")
# MAGIC connectionProperties.put("password", s"${jdbcPassword}")
# MAGIC 
# MAGIC val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# MAGIC connectionProperties.setProperty("Driver", driverClass)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 2: Reading the finance costcentre data
# MAGIC 
# MAGIC This creates a spark environment called costcentres_spark, which reads the costcentres table from Azure SQL

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val costcentres_spark = spark.read.jdbc(jdbcUrl, "costcentres", connectionProperties)
# MAGIC costcentres_spark.printSchema
# MAGIC //costcentres_spark.show(10)
# MAGIC //costcentres_spark.select("Costc_ID", "Executive_Directorate_T").groupBy("Executive_Directorate_T")
# MAGIC // example -- > display(employees_table.select("age", "salary").groupBy("age").avg("salary")) //
# MAGIC val summary = costcentres_spark.groupBy("Executive_Directorate_T").count().show()
# MAGIC print(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 3: Reading the Assemble teams  data
# MAGIC 
# MAGIC This creates a spark environment called assembleteams_spark, which reads the assemble teams table from Azure SQL

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val assembleteams_spark = spark.read.jdbc(jdbcUrl, "assembleteams", connectionProperties)
# MAGIC assembleteams_spark.printSchema

# COMMAND ----------

# MAGIC %scala
# MAGIC assembleteams_spark.createOrReplaceTempView("a")
# MAGIC costcentres_spark.createOrReplaceTempView("c")
# MAGIC 
# MAGIC val invalidCostCentreDataFrame = spark.sql("SELECT a.name, a.id AS assemble_id, a.description, a.disabled, c.Executive_Directorate_T, c.Directorate_T, c.Service_T, a.external_reference AS cost_centre FROM a LEFT JOIN c ON a.external_reference = c.Costc_ID WHERE a.external_reference != '' AND c.Executive_Directorate_T IS NULL ORDER BY Executive_Directorate_T")
# MAGIC invalidCostCentreDataFrame.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Write the table of Assemble teams without valid cost centres to the SQL server table
# MAGIC 
# MAGIC This section, takes the dataframe 'invalidCostCentreDataFrame' and  writes it to a table in SQL Server. Each time it runs, it will overwrite and existing table

# COMMAND ----------

# MAGIC %scala
# MAGIC invalidCostCentreDataFrame
# MAGIC   .write
# MAGIC   .mode(SaveMode.Overwrite) // <--- Overwrite the existing table
# MAGIC   .jdbc(jdbcUrl, "invalidcostcentres", connectionProperties)
