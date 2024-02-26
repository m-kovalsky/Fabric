import sempy
import sempy.fabric as fabric
import pandas as pd
import json

def get_lakehouse_details(lakehouseName = None, workspaceName = None):

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

    print("Below shows the Shared Expression for a Direct Lake semantic model connected to this lakehouse's SQL Endpoint\n")

    print('let\n\tdatabase = Sql.Database("' + sqlEPCS + '", "' + sqlepid + '")\nin\n\tdatabase')

    return df

get_lakehouse_details()