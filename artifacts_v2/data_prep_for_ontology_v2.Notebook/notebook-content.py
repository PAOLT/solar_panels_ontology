# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fed084ca-3f3c-4a20-8d46-29cfe5366d11",
# META       "default_lakehouse_name": "solar_panels_ontology_v2_lh_91b690baf496417fa7cc5adeda61854b",
# META       "default_lakehouse_workspace_id": "d8ed0771-df93-4eaa-8c89-33e2dc66b11c",
# META       "known_lakehouses": [
# META         {
# META           "id": "fed084ca-3f3c-4a20-8d46-29cfe5366d11"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Data preparation for solar_panels_ontology_v2

# MARKDOWN ********************

# ## Plant's snapshots
# 
# Join plant_record (FK to plant & timestamp) with snapshot tables (specific JSON field). Tables:
# - electrical_performance
# - mechanical_condition
# - environmental_conditions
# - operational_metrics

# CELL ********************

def sql_to_delta(sql, target_table_name, target_schema = "dbo", save_mode = 'overwrite'):
    # Save mode: "overwrite", "append", "ignore", or "error" (alias "errorifexists").

    df = spark.sql(sql)
    display(df.limit(3))

    full_table_name = f"{target_schema}.{target_table_name}"
    df.write.format("delta").mode(save_mode).saveAsTable(target_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### electrical_performance

# CELL ********************

sql_electrical_performance = '''SELECT snapkey, sitetie, snapts, elec_perf_snapshot 
FROM birdbench.solar_panel_large.plant_record pr
JOIN birdbench.solar_panel_large.electrical_performance ep ON pr.snapkey=ep.snaplink
'''

sql_to_delta(sql_electrical_performance, "electrical_performance_snapshot", target_schema = "dbo", save_mode = 'overwrite')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### environmental_conditions

# CELL ********************

sql_environmental_conditions = '''SELECT snapkey, sitetie, snapts, env_snapshot 
FROM birdbench.solar_panel_large.plant_record pr
JOIN birdbench.solar_panel_large.environmental_conditions ec ON pr.snapkey=ec.snapref
'''

sql_to_delta(sql_environmental_conditions, "environmental_conditions_snapshot", target_schema = "dbo", save_mode = 'overwrite')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### mechanical_condition

# CELL ********************

sql_mechanical_condition = '''
SELECT snapkey, sitetie, snapts, mech_health_snapshot
FROM birdbench.solar_panel_large.plant_record pr 
JOIN birdbench.solar_panel_large.mechanical_condition mc ON pr.snapkey=mc.snapmk
'''

sql_to_delta(sql_mechanical_condition, "mechanical_condition_snapshot", target_schema = "dbo", save_mode = 'overwrite')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### operational_metrics

# CELL ********************

sql_operational_metrics = '''
SELECT snapkey, sitetie, snapts, mtbfh, mttrh, maintcost, cleancost, replcost, revloss, optpot
FROM birdbench.solar_panel_large.plant_record pr 
JOIN birdbench.solar_panel_large.operational_metrics om ON pr.snapkey=om.snapops
'''

sql_to_delta(sql_operational_metrics, "operational_metrics_snapshot", target_schema = "dbo", save_mode = 'overwrite')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### alerts

# CELL ********************

sql_alert = '''
SELECT snapkey, sitetie, snapts, alrtstate, alrtcnt, maintprio, replprio
FROM birdbench.solar_panel_large.plant_record pr 
JOIN birdbench.solar_panel_large.alert a ON a.snapalrt=pr.snapkey
'''

sql_to_delta(sql_alert, "alerts", target_schema = "dbo", save_mode = 'overwrite')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
