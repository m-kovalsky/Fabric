import sempy
import sempy.fabric as fabric
import pandas as pd

def get_is_resident_model_size(datasetName, workspaceName = None):

    if workspaceName == None:
        workspaceId = fabric.get_workspace_id()
        workspaceName = fabric.resolve_workspace_name(workspaceId)

    # This dataframe is used to map Column IDs
    col = fabric.evaluate_dax(
            datasetName,
            """
            SELECT 
            [ID] AS [ColumnID]
            ,[TableID]
            ,[ExplicitName] AS [ColumnName]
            FROM $SYSTEM.TMSCHEMA_COLUMNS
            """)

    # This dataframe is used to map Table IDs
    tbl = fabric.evaluate_dax(
            datasetName,
            """
            SELECT 
            [ID] AS [TableID]
            ,[Name] AS [TableName]
            FROM $SYSTEM.TMSCHEMA_TABLES
            """)

    tblcol = pd.merge(col, tbl, on='TableID')

    # USED_SIZE is used to calculate Data Size, User Hierarchy Size, Column Hierarchy Size, Total Size
    cs = fabric.evaluate_dax(
            datasetName,
            """
            SELECT
            [DIMENSION_NAME]
            ,[TABLE_ID]
            ,[COLUMN_ID]
            ,[USED_SIZE]
            ,[SEGMENT_NUMBER]
            FROM $SYSTEM.DISCOVER_STORAGE_TABLE_COLUMN_SEGMENTS
            """)

    def parse_value(text):
        ind = text.rfind('(') + 1
        output = text[ind:]
        output = output[:-1]
        return output

    def parse_value2(text):
        ind = text.index('(') + 1
        indend = text.index(')')
        output = text[ind:indend]
        return output

    cs['ColumnID'] = cs['TABLE_ID'].apply(parse_value).astype('uint64')
    tblcolcs = pd.merge(cs[['DIMENSION_NAME', 'ColumnID', 'USED_SIZE', 'TABLE_ID','SEGMENT_NUMBER']], tblcol, on='ColumnID', how='left')

    # Used to calculate Dictionary Size, Temperature, Is Resident, Last Accessed
    dic = fabric.evaluate_dax(
            datasetName,
            """
            SELECT 
            [DIMENSION_NAME]
            ,[ATTRIBUTE_NAME]
            ,[DICTIONARY_SIZE]
            ,[ISROWNUMBER]
            ,[DICTIONARY_ISRESIDENT]
            ,[DICTIONARY_TEMPERATURE]
            ,[DICTIONARY_LAST_ACCESSED]
            ,[COLUMN_ENCODING]
            FROM $SYSTEM.DISCOVER_STORAGE_TABLE_COLUMNS
            WHERE [COLUMN_TYPE] = 'BASIC_DATA' 
            """)

    dfC = fabric.list_columns(datasetName)
    dfC['Hierarchy Size'] = None
    dfC['Dictionary Size'] = None
    dfC['Data Size'] = None
    dfC['Total Size'] = None
    dfC['Temperature'] = None
    dfC['Is Resident'] = None
    dfC['Last Accessed'] = None
    for index, row in dfC.iterrows():
        tableName = row['Table Name']
        columnName = row['Column Name']

        # Add value for Hierarchy Size
        filtered_cs = tblcolcs[(tblcolcs['TableName'] == tableName) & (tblcolcs['ColumnName'] == columnName) & (tblcolcs['TABLE_ID'].str.startswith("H$"))]
        sumval = filtered_cs['USED_SIZE'].sum()
        dfC.at[index, 'Hierarchy Size'] = sumval
        
        # Add value for Dictionary Size
        filtered_dic = dic[(dic['DIMENSION_NAME'] == tableName) & (dic['ATTRIBUTE_NAME'] == columnName) & (dic['ISROWNUMBER'] == 0)]
        dfC.at[index, 'Dictionary Size'] = filtered_dic['DICTIONARY_SIZE'].iloc[0]
        dfC.at[index, 'Is Resident'] = filtered_dic['DICTIONARY_ISRESIDENT'].iloc[0]
        dfC.at[index, 'Temperature'] = filtered_dic['DICTIONARY_TEMPERATURE'].iloc[0]
        dfC.at[index, 'Last Accessed'] = filtered_dic['DICTIONARY_LAST_ACCESSED'].iloc[0]

        # Add value for Data Size
        cs2 = cs[(~cs['TABLE_ID'].str.startswith("H$")) & (~cs['TABLE_ID'].str.startswith("R$")) & (~cs['TABLE_ID'].str.startswith("U$"))]
        cs2['ColumnID'] = cs2['COLUMN_ID'].apply(parse_value).astype('uint64')
        cs3 = pd.merge(cs2, col[['ColumnID', 'ColumnName']], on='ColumnID', how='left')
        cs4 = cs3[(cs3['ColumnName'] == columnName) & (cs3['DIMENSION_NAME'] == tableName)]
        dfC.at[index, 'Data Size'] = cs4['USED_SIZE'].iloc[0]       

    # Add value for Total Size
    dfC['Total Size'] = (dfC['Data Size'] + dfC['Dictionary Size'] + dfC['Hierarchy Size']).astype('uint64')

    dfC_filt = dfC[dfC['Is Resident']]
    total_size_sum = dfC_filt['Total Size'].sum()
    
    return total_size_sum
    

get_is_resident_model_size('') #Enter semantic model name