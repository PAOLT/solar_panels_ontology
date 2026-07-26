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

# ### Save CSV files as Delta tables
# 
# This is for all the Bird Bench databases and tables

# CELL ********************

from pyspark.sql import functions as F
import re

base_path = "Files/db_large_exports"
lakehouse_name = "birdbench"  # default lakehouse attached to this notebook

# IMPORTANT: allow writing pre-1900 timestamps / pre-1582 dates to Parquet/Delta
# This follows Spark's guidance in the error message and ensures compatibility
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

# helper to make valid SQL identifiers
def to_identifier(name: str):
    # lowercase, replace non-alphanumeric with underscore, collapse repeats, trim underscores
    cleaned = re.sub(r"[^a-zA-Z0-9]", "_", name.strip())
    cleaned = re.sub(r"_+", "_", cleaned)
    cleaned = cleaned.strip("_")
    if not cleaned:
        raise ValueError(f"Cannot convert '{name}' to a valid identifier")
    return cleaned.lower()

# list top-level folders under db_large_exports
folders = [f.name for f in notebookutils.fs.ls(base_path) if f.isDir]
folders = [f for f in folders if f[0] >= 's']

print(f"Found folders: {folders}")

for folder in folders:
    schema_name = to_identifier(folder)
    print(f"\n=== Processing folder '{folder}' as schema '{schema_name}' ===")

    # create schema if not exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{lakehouse_name}`.`{schema_name}`")

    folder_path = f"{base_path}/{folder}"
    entries = notebookutils.fs.ls(folder_path)
    csv_files = [e.name for e in entries if not e.isDir and e.name.lower().endswith(".csv")]

    if not csv_files:
        print(f"  No CSV files found in {folder_path}, skipping.")
        continue

    for csv_file in csv_files:
        table_base = csv_file[:-4] if csv_file.lower().endswith(".csv") else csv_file
        table_name = to_identifier(table_base)
        print(f"  Creating table '{lakehouse_name}.{schema_name}.{table_name}' from file '{csv_file}'")

        file_path = f"{folder_path}/{csv_file}"

        df = (
            spark.read
                 .option("header", "true")
                 .option("inferSchema", "true")
                 .option("multiLine", "true")
                 .option("escape", "\"")
                 .csv(file_path)
        )

        # write as managed Delta table into the lakehouse
        full_table_name = f"`{lakehouse_name}`.`{schema_name}`.`{table_name}`"
        (
            df.write
              .mode("overwrite")
              .format("delta")
              .saveAsTable(full_table_name)
        )

print("\nCompleted creating schemas and tables from CSV exports.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Rename tables

# CELL ********************

from pyspark.sql import functions as F

# Parameters: set these as needed
lakehouse_name = "birdbench"   # target lakehouse
schema_name = "solar_panel_large"         # target schema
prefix_to_remove = "public_"   # prefix to strip from table names

# List tables in the target schema
tables_df = spark.sql(f"SHOW TABLES IN `{lakehouse_name}`.`{schema_name}`")

# Filter tables whose names start with the given prefix
tables_to_rename = (
    tables_df
    .filter(F.col("tableName").startswith(prefix_to_remove))
    .select("tableName")
    .distinct()
    .collect()
)

print(f"Found {tables_df.count()} tables. Renaming {len(tables_to_rename)}")

for row in tables_to_rename:
    old_name = row["tableName"]
    new_name = old_name[len(prefix_to_remove):]

    # Safety check: ensure we don't overwrite an existing table
    existing = spark.sql(
        f"SHOW TABLES IN `{lakehouse_name}`.`{schema_name}` LIKE '{new_name}'"
    ).count()

    if existing > 0:
        print(
            f"SKIP: `{lakehouse_name}`.`{schema_name}`.`{old_name}` "
            f"-> `{new_name}` (target already exists)"
        )
        continue

    sql_stmt = (
        f"ALTER TABLE `{lakehouse_name}`.`{schema_name}`.`{old_name}` "
        f"RENAME TO `{lakehouse_name}`.`{schema_name}`.`{new_name}`"
    )
    print(f"EXEC: {sql_stmt}")
    spark.sql(sql_stmt)

    print("\nCompleted renaming tables.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
