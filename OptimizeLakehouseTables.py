from delta.tables import *

tables = spark.catalog.listTables()

for t in tables:
    tableName = t.name
    delta_table_path = f"Tables/{tableName}"
    deltaTable = DeltaTable.forPath(spark, delta_table_path)
    deltaTable.optimize().executeCompaction()
    print("The '" + tableName + "' table has been optimized.")
