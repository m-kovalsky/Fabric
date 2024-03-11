import sempy
import sempy.fabric as fabric
import pandas as pd
from tqdm.auto import tqdm

def warm_direct_lake_cache(datasetName, perspectiveName, addKeys = False):

    dfPersp = fabric.list_perspectives(dataset = datasetName)
    dfPersp = dfPersp[(dfPersp['Perspective Name'] == perspectiveName) & (dfPersp['Object Type'] == "Column")]
    dfPersp['DAX Object Name'] = "'" + dfPersp['Table Name'] + "'[" + dfPersp['Object Name'] + "]"

    column_values = dfPersp['DAX Object Name'].tolist()

    if addKeys:
        unique_table_names = dfPersp['Table Name'].unique()
        dfR = fabric.list_relationships(datasetName)
        filtered_dfR = dfR[dfR['From Table'].isin(unique_table_names) & dfR['To Table'].isin(unique_table_names)]

        from_objects = [
            "'" + row['From Table'] + "'[" + row['From Column'] + "]"
            for _, row in filtered_dfR.iterrows()
        ]
        to_objects = [
            "'" + row['To Table'] + "'[" + row['To Column'] + "]"
            for _, row in filtered_dfR.iterrows()
        ]

        merged_list = column_values + from_objects + to_objects
        merged_list_unique = list(set(merged_list))

    else:
        merged_list_unique = column_values

    df = pd.DataFrame(merged_list_unique, columns=['DAX Object Name'])
    df[['Table Name', 'Column Name']] = df['DAX Object Name'].str.split('[', expand=True)
    df['Table Name'] = df['Table Name'].str[1:-1]
    df['Column Name'] = df['Column Name'].str[0:-1]

    tbls = list(set(value.split('[')[0] for value in merged_list_unique))

    for tableName in (bar := tqdm(tbls)):
        filtered_list = [value for value in merged_list_unique if value.startswith(f"{tableName}[")]
        bar.set_description(f"Warming {tableName}...")
        css = ','.join(map(str, filtered_list))
        dax = """EVALUATE TOPN(1,SUMMARIZECOLUMNS(""" + css + "))"""
        x = fabric.evaluate_dax(datasetName, dax)            
    
    print(f"The following columns have been put into memory:")
    
    return df

warm_direct_lake_cache('AdvWorks', 'DirectLakeWarm', True) #Enter dataset name, Enter perspective containing objects for warming; Setting addKeys paramter to True will ensure foreign/primary keys from tables in the perspective are also put in memory
