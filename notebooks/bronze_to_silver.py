# Databricks notebook source
dbutils.fs.ls("/mnt/data/bronze/dataset_imoveis")

# COMMAND ----------

path = "/mnt/data/bronze/dataset_imoveis"
df = spark.read.format("delta").load(path)

# COMMAND ----------

display(df)

# COMMAND ----------

display(df.select("anuncio.*"))

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import StructType

# Função recursiva para expandir a estrutura aninhada, mas dentro de uma coluna específica (neste caso, 'anuncio')
def flatten_struct_fields(schema, prefix=""):
    fields = []
    for field in schema.fields:
        field_name = f"{prefix}.{field.name}" if prefix else field.name
        if isinstance(field.dataType, StructType):
            fields += flatten_struct_fields(field.dataType, prefix=field_name)
        else:
            fields.append(col(field_name).alias(field_name.replace(".", "_")))
    return fields

# Expanda os campos do DataFrame dentro da coluna 'anuncio'
fields = flatten_struct_fields(df.schema['anuncio'].dataType, prefix="anuncio")
df_flat = df.select(col("*"), *fields).drop("anuncio")

# Exiba o DataFrame resultante
df_flat.show()

# COMMAND ----------

display(df_flat)

# COMMAND ----------

# MAGIC %md
# MAGIC # maneira mais fácil

# COMMAND ----------

df_expanded = df.select("anuncio.*", "anuncio.endereco.*")

# COMMAND ----------

display(df_expanded)

# COMMAND ----------

df_silver_2 = df_expanded.drop("endereco", "caracteristicas")

# COMMAND ----------

display(df_silver_2)

# COMMAND ----------

df_silver = df_flat.drop("anuncio_caracteristicas", "anuncio_id")
display(df_silver)

# COMMAND ----------

df_silver = df_silver_2

# COMMAND ----------

path = "dbfs:/mnt/data/silver/dataset_imoveis"
df_silver.write.format("delta").mode("overwrite").save(path)

# COMMAND ----------

dbutils.fs.ls(path)

# COMMAND ----------


