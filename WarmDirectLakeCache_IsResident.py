import sempy
import sempy.fabric as fabric
import pandas as pd
import time

def warm_direct_lake_cache_is_resident(datasetName):

    # Identify columns which are currently in memory (Is Resident = True)
    dfC = fabric.list_columns(datasetName, extended = True)
    dfC['DAX Object Name'] = "'" + dfC['Table Name'] + "'[" + dfC['Column Name'] + "]"
    dfC_filtered = dfC[dfC['Is Resident']]

    # Refresh/frame dataset
    requestID = fabric.refresh_dataset(datasetName, refresh_type = 'full')

    while True:
        requestDetails = fabric.get_refresh_execution_details(datasetName,requestID)
        status = requestDetails.status

        # Check if the refresh has completed
        if status == 'Completed':
            break

        time.sleep(3)

    print(f"Refresh for the '{datasetName}' semantic model is complete.")

    time.sleep(2)

    tbls = dfC_filtered['Table Name'].unique()
    column_values = dfC_filtered['DAX Object Name'].tolist()

    # Run basic query to get columns into memory; completed one table at a time (so as not to overload the capacity)
    for tableName in tbls:    
        css = ','.join(map(str, column_values))
        dax = """EVALUATE TOPN(1,SUMMARIZECOLUMNS(""" + css + "))"""
        x = fabric.evaluate_dax(datasetName, dax)        
            
    print(f"The following columns have been put into memory:")
    return dfC_filtered[['Table Name', 'Column Name', 'Is Resident', 'Temperature']]

warm_direct_lake_cache_is_resident('') #Enter dataset name