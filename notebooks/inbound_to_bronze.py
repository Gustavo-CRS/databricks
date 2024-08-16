# Databricks notebook source
dbutils.fs.ls("/mnt/data/inbound")

# COMMAND ----------

filePath = "dbfs:/mnt/data/inbound/dados_brutos_imoveis.json"
data = spark.read.json(filePath)

# COMMAND ----------

data.show()

# COMMAND ----------

data.printSchema()

# COMMAND ----------

display(data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Removendo colunas

# COMMAND ----------

data = data.drop("imagens", "usuario")
display(data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criando Id para cada anúncio

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df_bronze = data.withColumn("id", col("anuncio.id"))

# COMMAND ----------

display(df_bronze)

# COMMAND ----------

df_bronze_path = "dbfs:/mnt/data/bronze/dataset_imoveis"
df_bronze.write.format("delta").mode(saveMode="Overwrite").save(df_bronze_path)

# COMMAND ----------

dbutils.fs.ls("/mnt/data/bronze/dataset_imoveis")

# COMMAND ----------

df_endereco = df_bronze.select("anuncio.endereco")
display(df_endereco)

# COMMAND ----------


