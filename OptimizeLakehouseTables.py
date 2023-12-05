# This code snippet runs the OPTIMIZE function against all the tables in your lakehouse (in order to improve query performance)

from delta.tables import *
from tqdm.auto import tqdm

tables = spark.catalog.listTables()
tableCount = len(tables)

i=1
for t in (bar := tqdm(tables)):
    tableName = t.name    
    bar.set_description(f"Optimizing {tableName}")
    delta_table_path = f"Tables/{tableName}"
    deltaTable = DeltaTable.forPath(spark, delta_table_path)
    deltaTable.optimize().executeCompaction()
    print(f"The '{tableName}' table has been optimized. ({str(i)}/{str(tableCount)})")
    i+=1