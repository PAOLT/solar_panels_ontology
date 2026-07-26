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

# ### Manage delta tables

# CELL ********************

# MAGIC %%sql
# MAGIC -- creating timeseries tables (plant records)
# MAGIC 
# MAGIC SELECT plant_record.*, 
# MAGIC 	operational_metrics.mtbfh, 
# MAGIC 	operational_metrics.mttrh, 
# MAGIC 	operational_metrics.maintcost, 
# MAGIC 	operational_metrics.cleancost, 
# MAGIC 	operational_metrics.replcost, 
# MAGIC 	operational_metrics.revloss, 
# MAGIC 	operational_metrics.optpot,
# MAGIC 	electrical_performance.elec_perf_snapshot,
# MAGIC 	mechanical_condition.mech_health_snapshot,
# MAGIC 	environmental_conditions.env_snapshot,
# MAGIC 	alert.alrtstate, 
# MAGIC 	alert.alrtcnt, 
# MAGIC 	alert.maintprio, 
# MAGIC 	alert.replprio
# MAGIC FROM birdbench.solar_panel_large.plant_record
# MAGIC JOIN birdbench.solar_panel_large.operational_metrics ON snapops = snapkey
# MAGIC JOIN birdbench.solar_panel_large.electrical_performance ON snaplink = snapkey
# MAGIC JOIN birdbench.solar_panel_large.mechanical_condition ON snapmk = snapkey
# MAGIC JOIN birdbench.solar_panel_large.environmental_conditions ON snapref = snapkey
# MAGIC JOIN birdbench.solar_panel_large.alert ON snapalrt = snapkey

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# creating timeseries tables (plant records * snapshots)

sql_snaps = ''' 
SELECT plant_record.*, 
	electrical_performance.elec_perf_snapshot,
	mechanical_condition.mech_health_snapshot,
	environmental_conditions.env_snapshot
FROM birdbench.solar_panel_large.plant_record
JOIN birdbench.solar_panel_large.electrical_performance ON snaplink = snapkey
JOIN birdbench.solar_panel_large.mechanical_condition ON snapmk = snapkey
JOIN birdbench.solar_panel_large.environmental_conditions ON snapref = snapkey
'''

spark.sql(sql_snaps).write.saveAsTable("solar_panel_large.plant_record_snapshots")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# creating timeseries tables (plant records * alerts)

sql_alerts = ''' 
SELECT plant_record.*, 
	alert.alrtstate, 
	alert.alrtcnt, 
	alert.maintprio, 
	alert.replprio,
	work_orders.OrderNum
FROM birdbench.solar_panel_large.plant_record
JOIN birdbench.solar_panel_large.alert ON snapalrt = snapkey
JOIN birdbench.solar_panel_large.work_orders ON snapalrt = TriggeringAlert
'''

spark.sql(sql_alerts).write.saveAsTable("solar_panel_large.plant_record_alerts")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # creating timeseries tables (plant records * environmental_conditions)

# sql_env_cond = ''' 
# SELECT plant_record.*, environmental_conditions.env_snapshot 
# FROM birdbench.solar_panel_large.plant_record
# JOIN birdbench.solar_panel_large.environmental_conditions ON snapref = snapkey
# '''

# spark.sql(sql_env_cond).write.saveAsTable("solar_panel_large.plant_record_environmental_conditions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # creating timeseries tables (plant records * mechanical_condition)

# sql_mech_cond = ''' 
# SELECT plant_record.*, mechanical_condition.mech_health_snapshot
# FROM birdbench.solar_panel_large.plant_record
# JOIN birdbench.solar_panel_large.mechanical_condition ON snapmk = snapkey
# '''

# spark.sql(sql_mech_cond).write.saveAsTable("solar_panel_large.plant_record_mechanical_conditions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # creating timeseries tables (plant records * electrical_performance)

# sql_el_perf = '''
# SELECT plant_record.*, electrical_performance.elec_perf_snapshot
# FROM birdbench.solar_panel_large.plant_record
# JOIN birdbench.solar_panel_large.electrical_performance ON snaplink = snapkey
# '''

# spark.sql(sql_el_perf).write.saveAsTable("solar_panel_large.plant_record_electrical_performance")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# creating timeseries tables (plant records * operational_metrics)

sql_oper_metrics = '''
SELECT plant_record.*, 
	operational_metrics.mtbfh, 
	operational_metrics.mttrh, 
	operational_metrics.maintcost, 
	operational_metrics.cleancost, 
	operational_metrics.replcost, 
	operational_metrics.revloss, 
	operational_metrics.optpot
FROM birdbench.solar_panel_large.plant_record
JOIN birdbench.solar_panel_large.operational_metrics ON snapops = snapkey
'''

spark.sql(sql_oper_metrics).write.saveAsTable("solar_panel_large.plant_record_operational_metrics")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from solar_panel_large.plant_record_alerts

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
