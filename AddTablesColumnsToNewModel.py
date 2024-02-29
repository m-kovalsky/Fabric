import sempy
import sempy.fabric as fabric
import pandas as pd
import numpy as np
sempy.fabric._client._utils._init_analysis_services()
from sempy.fabric._cache import _get_or_create_workspace_client
from sempy.fabric._client._connection_mode import ConnectionMode
import Microsoft.AnalysisServices.Tabular as TOM
from sempy.fabric._client import DatasetXmlaClient
from notebookutils import mssparkutils
import System

datasetName = '' # Original semantic model name
newDatasetName = '' # New semantic model name
workspaceId = fabric.get_workspace_id()
workspaceName = fabric.resolve_workspace_name(workspaceId)
tom_server = _get_or_create_workspace_client(workspaceName).get_dataset_client(newDatasetName, ConnectionMode.XMLA)._get_connected_dataset_server(readonly=False)

def list_tables(datasetName, workspaceName = None):

    if workspaceName == None:
        workspaceId = fabric.get_workspace_id()
        workspaceName = fabric.resolve_workspace_name(workspaceId)

    workspace_client = _get_or_create_workspace_client(workspaceName)
    ds = workspace_client.get_dataset(datasetName)
    m = ds.Model

    header = pd.DataFrame(columns=['Name', 'Type', 'Hidden', 'Data Category', 'Description', 'Refresh Policy', 'Source Expression'])
    df = pd.DataFrame(header)

    for t in m.Tables:
        tableType = "Table"
        rPolicy = bool(t.RefreshPolicy)
        sourceExpression = None
        if str(t.CalculationGroup) != "None":
            tableType = "Calculation Group"
        else:
            for p in t.Partitions:
                if str(p.SourceType) == "Calculated":
                    tableType = "Calculated Table"

        if rPolicy:
            sourceExpression = t.RefreshPolicy.SourceExpression

        new_data = {'Name': t.Name, 'Type': tableType,'Hidden': t.IsHidden, 'Data Category': t.DataCategory, 'Description': t.Description, 'Refresh Policy': rPolicy, 'Source Expression': sourceExpression}
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df

def get_shared_expression(lakehouseName = None, workspaceName = None):

    header = pd.DataFrame(columns=['Lakehouse Name', 'Lakehouse ID', 'Workspace Name', 'Workspace ID', 'OneLake Tables Path', 'OneLake Files Path', 'SQL Endpoint Connection String', 'SQL Endpoint ID', 'SQL Endpoint Provisioning Status'])
    df = pd.DataFrame(header)

    if workspaceName == None:
        workspaceID = fabric.get_workspace_id()
        workspaceName = fabric.resolve_workspace_name(workspaceID)
    if lakehouseName == None:
        lakehouseID = fabric.get_lakehouse_id()
    else:
        dfItems = fabric.list_items()
        dfItems = dfItems[dfItems['Display Name'] == lakehouseName and dfItems['Type'] == 'Lakehouse']
        lakehouseID = dfItems['Id'].iloc[0]
    
    client = fabric.FabricRestClient()

    #https://learn.microsoft.com/rest/api/fabric/articles/item-management/properties/lakehouse-properties
    response = client.get(f"/v1/workspaces/{workspaceID}/lakehouses/{lakehouseID}")
    responseJson = response.json()
    lakehouseName = responseJson['displayName']
    prop = responseJson['properties']
    oneLakeTP = prop['oneLakeTablesPath']
    oneLakeFP = prop['oneLakeFilesPath']
    sqlEPCS = prop['sqlEndpointProperties']['connectionString']
    sqlepid = prop['sqlEndpointProperties']['id']
    sqlepstatus = prop['sqlEndpointProperties']['provisioningStatus']

    new_data = {'Lakehouse Name': lakehouseName, 'Lakehouse ID': lakehouseID, 'Workspace Name': workspaceName, 'Workspace ID': workspaceID, 'OneLake Tables Path': oneLakeTP, 'OneLake Files Path': oneLakeFP, 'SQL Endpoint Connection String': sqlEPCS, 'SQL Endpoint ID': sqlepid, 'SQL Endpoint Provisioning Status': sqlepstatus}
    df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)  

    x = 'let\n\tdatabase = Sql.Database("' + sqlEPCS + '", "' + sqlepid + '")\nin\n\tdatabase'

    return x

# Check that lakehouse is attachd to the notebook
mounts = pd.DataFrame(mssparkutils.fs.mounts())
mounts_filt = mounts[mounts['storageType'] == 'Lakehouse']

if len(mounts_filt) == 1:
    print('Lakehouse attached to notebook\n')

    shEx = get_shared_expression()

    dfC = fabric.list_columns(datasetName)
    dfT = list_tables(datasetName)

    for d in tom_server.Databases:
        if d.Name == newDatasetName:
            print(f"Updating '{d.Name}' based on '{datasetName}'...")
            m = d.Model
            print(f"\nCreating shared expression parameter...")
            exp = TOM.NamedExpression()
            eName = 'DatabaseQuery'
            exp.Name = eName
            exp.Kind = TOM.ExpressionKind.M
            exp.Expression = shEx
            if not any(e.Name == eName for e in m.Expressions):
                m.Expressions.Add(exp)
                print(f"'{eName}' shared expression has been added.")

            for tName in dfC['Table Name'].unique():
                tType = dfT.loc[(dfT['Name'] == tName), 'Type'].iloc[0]
                tDC = dfC.loc[(dfC['Table Name'] == tName), 'Data Category'].iloc[0]
                tDesc = dfC.loc[(dfC['Table Name'] == tName), 'Description'].iloc[0]

                # Create the table with its columns for regular tables that do not already exist in the model
                if tType == 'Table' and not any(t.Name == tName for t in m.Tables):
                    tbl = TOM.Table()        
                    tbl.Name = tName
                    tbl.DataCategory = tDC
                    tbl.Description = tDesc

                    ep = TOM.EntityPartitionSource()
                    ep.Name = tName
                    ep.EntityName = tName.replace(' ', '_') #lakehouse table names use underscores instead of spaces
                    ep.ExpressionSource = exp

                    part = TOM.Partition()
                    part.Name = tName
                    part.Source = ep
                    part.Mode = TOM.ModeType.DirectLake

                    tbl.Partitions.Add(part)

                    columns_in_table = dfC.loc[dfC['Table Name'] == tName, 'Column Name'].unique()
               
                    print(f"\nCreating columns for '{tName}' table...")           
                    for cName in columns_in_table:
                        scName = dfC.loc[(dfC['Table Name'] == tName) & (dfC['Column Name'] == cName), 'Source'].iloc[0]
                        cType = dfC.loc[(dfC['Table Name'] == tName) & (dfC['Column Name'] == cName), 'Type'].iloc[0]
                        cHid = bool(dfC.loc[(dfC['Table Name'] == tName) & (dfC['Column Name'] == cName), 'Hidden'].iloc[0])
                        cDataType = dfC.loc[(dfC['Table Name'] == tName) & (dfC['Column Name'] == cName), 'Data Type'].iloc[0]

                        if cType == 'Data' and not any(t.Name == tName and c.Name == cName for t in m.Tables for c in t.Columns):
                            col = TOM.DataColumn()
                            col.Name = cName
                            col.IsHidden = cHid
                            col.SourceColumn = scName.replace(' ', '_') #lakehouse column names use underscores instead of spaces
                            col.DataType = System.Enum.Parse(TOM.DataType, cDataType)

                            tbl.Columns.Add(col)
                            print(f"The '{tName}'[{cName}] column has been added.")
               
                    m.Tables.Add(tbl)
                    print(f"The '{tName}' table has been added.")

            m.SaveChanges()
            print(f"\nAll regular tables and columns have been added to the model.")

else:
    print('Lakehouse not attached to notebook. Please add your lakehouse to this notebook.')
    print(f"In the 'Explorer' window to the left, click 'Lakehouses' to add your lakehouse to this notebook")
    print(f"\nLearn more here: https://learn.microsoft.com/fabric/data-engineering/lakehouse-notebook-explore#add-or-remove-a-lakehouse")