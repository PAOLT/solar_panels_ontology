# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a96b8679-a934-40dd-9266-dcd36cac9629",
# META       "default_lakehouse_name": "birdbench",
# META       "default_lakehouse_workspace_id": "d8ed0771-df93-4eaa-8c89-33e2dc66b11c",
# META       "known_lakehouses": [
# META         {
# META           "id": "a96b8679-a934-40dd-9266-dcd36cac9629"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Create solar panels KB table

# CELL ********************

from pyspark.sql.functions import col, struct, to_json
file_path = "Files/livesqlbench-large-v1/solar_panel_large/solar_panel_large_kb.jsonl"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark_df = spark.read.json(file_path, multiLine=False)

rename_map = {
    "children_knowledge": "children_knowledge_id",
    "knowledge": "knowledge_key",
    "id": "knowledge_id",
}

for old_name, new_name in rename_map.items():
    spark_df = spark_df.withColumnRenamed(old_name, new_name)

spark_df = spark_df.withColumn(
    "knowledge",
    to_json(
        struct(
            col("knowledge_key").alias("knowledge"),
            col("description").alias("description"),
            col("definition").alias("definition")
        )
    )
)

spark_df = spark_df.select(["knowledge_id", "knowledge_key", "knowledge", "type", "children_knowledge_id"])

display(spark_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark_df.write.mode("overwrite").saveAsTable("dbo.solar_panels_kb")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM birdbench.dbo.solar_panels_kb LIMIT 10")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
