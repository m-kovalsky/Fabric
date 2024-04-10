# Fabric
The following code snippets may be executed in a [Microsoft Fabric notebook](https://learn.microsoft.com/fabric/data-engineering/how-to-use-notebook). These scripts primarily leverage the semantic-link (a.k.a. SemPy) library.

This repo is intended to help business intelligence analysts/developers and data scientists become more familiar with notebooks (and therefore Python) in Microsoft Fabric and the potential benefits of using the Semantic Link library.

## Requirements
Semantic-link library version 0.7.2 or higher

## Load the [Semantic-Link](https://pypi.org/project/semantic-link/) library inside of your notebook
```python
%pip install semantic-link
```

## Load Semantic-link into a custom Fabric environment
An even better way to ensure the Semantic-link library is available in your workspace/notebooks is to load it as a library in a custom Fabric environment. If you do this, you will not have to run the above '%pip install semantic-link' code every time in your notebook. Please follow the steps below to create an environment and load the Semantic-Link library.

#### Create a custom environment
1. Navigate to your Fabric workspace
2. Click 'New' -> More options
3. Within 'Data Science', click 'Environment'
4. Name your environment, click 'Create'

#### Add Semantic-Link as a library to the environment
1. Within 'Public Libraries', click 'Add from PyPI'
2. Enter 'Semantic-link' in the text box under 'Library'
3. Click 'Save' at the top right of the screen
4. Click 'Publish' at the top right of the screen
5. Click 'Publish All'

#### Update your notebook to use the new environment (*must wait for the environment to finish publishing*)
1. Navigate to your Notebook
2. Select your newly created environment within the 'Environment' drop down in the navigation bar at the top of the notebook

## Packages, functions and SemPy help

#### Useful packages for python notebooks (SemPy and more)
```python
import sempy
import sempy.fabric as fabric
import pandas as pd
import numpy as np
import pyspark
import pyspark.sql.functions as F
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import os
from delta.tables import DeltaTable
from powerbiclient import Report
```

#### Show the directory of all SemPy functions
```python
import sempy
import sempy.fabric
dir(sempy.fabric)
```

#### Show all functions in a given Python library
```python
import inspect
import sempy
import sempy.fabric as fabric

all_members = inspect.getmembers(fabric) #Enter the library alias
for name, member in all_members:
    if inspect.isfunction(member):
        print(name)
```

### Show all functions in a given Python library and their respective parameters
```python
import inspect
import sempy
import sempy.fabric as fabric

functions = inspect.getmembers(fabric,inspect.isfunction) #Enter the library alias

for name, func in functions:
    signature = inspect.signature(func)
    parameters = list(signature.parameters.keys())
    print(f"{name} : {parameters}\n")
```

#### Show useful information about a given function
```python
import sempy
import sempy.fabric as fabric
help(fabric.list_datasets) #Replace 'list_datasets' within any function shown in dir(sempy.fabric) output
```

#### Show the version of a Python library within your notebook
```python
import pkg_resources
library_name = 'semantic-link' #Enter the name of the library
version = pkg_resources.get_distribution(library_name).version
version
```

#### Show the latest version available of a Python library
```python
import requests
library_name = 'semantic-link' #Enter the name of the library
url = f"https://pypi.org/pypi/{library_name}/json"
response = requests.get(url)
data = response.json()
latest_version = data["info"]["version"]
print(f"The latest version of '{library_name}' is: {latest_version}")
```

#### Identify if you have the latest version of a given library installed in your notebook
```python
import pkg_resources
import requests

library_name = 'semantic-link' #Enter the name of the library
version = pkg_resources.get_distribution(library_name).version

url = f"https://pypi.org/pypi/{library_name}/json"
response = requests.get(url)
data = response.json()
latest_version = data["info"]["version"]

if version == latest_version:
    print(f"You have the latest version of '{library_name}' installed.")
else:
    print(f"A new version '{latest_version}' of the '{library_name}' library is available.")
```

#### Show all available libraries in your notebook
```python
import pkg_resources
import pandas as pd
installed_packages = [pkg.key for pkg in pkg_resources.working_set]
title = 'Installed Packages'
df = pd.DataFrame({title: installed_packages})
display(df)
```

#### Refresh the TOM cache. If the semantic model has been updated during your notebook session, run this script to ensure you get the latest model metadata.
```python
import sempy
import sempy.fabric as fabric
fabric.refresh_tom_cache()
```

## Capacities

#### Show a list of all accessible capacities
```python
import sempy
import sempy.fabric as fabric
x = fabric.list_capacities()
x
```

#### Gets the Artifact ID (of the notebook)
```python
import sempy
import sempy.fabric as fabric
x = fabric.get_artifact_id()
x
```

## Lakehouse

#### Gets the Lakehouse ID from the current lakehouse
```python
import sempy
import sempy.fabric as fabric
x = fabric.get_lakehouse_id()
x
```

#### Gets the SQL Endpoint for a given workspace/lakehouse
```python
import sempy
import sempy.fabric as fabric
import pandas as pd

def get_sql_endpoint(workspaceName = None, lakehouseName = None):

    if workspaceName == None:        
        workspaceID = fabric.get_workspace_id()
    else:
        workspaceID = fabric.resolve_workspace_id(workspaceName)
    if lakehouseName == None:
        lakehouseID = fabric.get_lakehouse_id()
    else:
        dfItems = fabric.list_items()
        dfItems = dfItems[dfItems['Display Name'] == lakehouseName and dfItems['Type'] == 'Lakehouse']
        lakehouseID = dfItems['Id'].iloc[0]

    workspaceID = fabric.get_workspace_id()
    lakehouseID = fabric.get_lakehouse_id()
    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspaceID}/lakehouses/{lakehouseID}")
    responseJson = response.json()
    sqlEndpoint = responseJson['properties']['sqlEndpointProperties']['connectionString']
    
    return sqlEndpoint

get_sql_endpoint()
```

#### Show a list of the tables in your lakehouse using the [List Tables API](https://learn.microsoft.com/rest/api/fabric/lakehouse/tables/list-tables?tabs=HTTP)
```python
import sempy
import sempy.fabric as fabric
import pandas as pd
import json

def get_lakehouse_tables(workspaceId = None, lakehouseId = None):

    if workspaceId == None:
        workspaceId = fabric.get_workspace_id()
    if lakehouseId == None:
        lakehouseId = fabric.get_lakehouse_id()

    client = fabric.FabricRestClient()
    wName = fabric.resolve_workspace_name(workspaceId)
    response = client.get(f"/v1/workspaces/{workspaceId}/lakehouses/{lakehouseId}/tables")
    tableList = response.json()['data']
    dfItems = fabric.list_items()
    dfItems = dfItems[dfItems['Id'] == lakehouseId]
    lakehouseName = dfItems['Display Name'].iloc[0]

    df = pd.DataFrame({'Workspace Name': [], 'Lakehouse Name': [], 'Table Name': [], 'Type': [], 'Location': [], 'Format': []})

    for t in tableList:
        tName = t['name']
        tType = t['type']
        tLocation = t['location']
        tFormat = t['format']

        new_data = {'Workspace Name': wName, 'Lakehouse Name': lakehouseName, 'Table Name': tName, 'Type': tType, 'Location': tLocation, 'Format': tFormat}
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df

get_lakehouse_tables()
```

#### Load a parquet or csv file within your lakehouse as a delta table in your lakehouse
```python
import sempy
import sempy.fabric as fabric
import pandas as pd
import os

def load_table(tablename, source, mode = None, workspaceId = None, lakehouseId = None):

    #Mode values: Overwrite; Append
    if workspaceId == None:
        workspaceId = fabric.get_workspace_id()
    if lakehouseId == None:
        lakehouseId = fabric.get_lakehouse_id()
    if mode == None:
        mode = 'Overwrite'
    if not mode in ['Overwrite', 'Append']:
        return print("Invalid Mode value. Mode must be either 'Overwrite' or 'Append'.")

    payload = None
    file_extension = os.path.splitext(source)[1]
    
    if file_extension == '.csv':
        payload = {
        "relativePath": source,
        "pathType": "File",
        "mode": mode,
        "formatOptions": {
            "format": "Csv",
            "header": "true",
            "delimiter": ","
        }
        }
    elif file_extension == '.parquet':
        payload = {
        "relativePath": source,
        "pathType": "File",
        "mode": mode,
        "formatOptions": {
            "format": "Parquet",
            "header": "true"
        }
        }
    else:
        return print("Invalid file extension. Only .csv and .parquet are supported.")

    client = fabric.FabricRestClient()
    response = client.post(f"/v1/workspaces/{workspaceId}/lakehouses/{lakehouseId}/tables/{tableName}/load",json= payload)
    
    return response

load_table(
     tablename = 'TestFile' #Enter the name of the table to be created
    ,source = 'Files/Folder/Myfilename.parquet' #Enter the file path of your Parquet file
    )
```

## Workspaces

#### Show a list of all objects in your workspace
```python
import sempy
import sempy.fabric as fabric
x = fabric.list_items()
x
```

#### Shows a list of your accessible workspaces, sorted alphabetically
```python
import sempy
import sempy.fabric as fabric
x = fabric.list_workspaces().sort_values(by='Name', ascending=True)
x
```

#### Filter to a particular workspace
```python
import sempy
import sempy.fabric as fabric
workspaceName = '' #Enter the workspace name to be used as a filter
x = fabric.list_workspaces()
filter_condition = [workspaceName]
x = x[x['Name'].isin(filter_condition)]
x
```

#### Filter to a particular workspace and extract the value
```python
import sempy
import sempy.fabric as fabric
workspaceName = '' #Enter the workspace name to be used as a filter
x = fabric.list_workspaces()
filter_condition = [workspaceName]
x = x[x['Name'].isin(filter_condition)]
y = x["Id"].iloc[0]
z = fabric.resolve_workspace_name(y)
z
```

#### Show the Capacity SKU, Region, State for your workspaces
```python
import sempy
import sempy.fabric as fabric
import pandas as pd

dfC = fabric.list_capacities()
dfW = fabric.list_workspaces().sort_values(by='Name', ascending=True)
dfC.rename(columns={'Id': 'Capacity Id'}, inplace=True)
dfC['Capacity Id'] = dfC['Capacity Id'].str.upper()
x = pd.merge(dfW, dfC[['Capacity Id', 'Sku', 'Region', 'State']], on='Capacity Id', how='inner')
x
```

#### Find the workspace ID for a given workspace name
```python
import sempy
import sempy.fabric as fabric
x = '' #Enter the workspace name
id = fabric.resolve_workspace_id(x)
id
```

#### Find the workspace name for a given workspace ID
```python
import sempy
import sempy.fabric as fabric
id = '' #Enter the workspace ID
x = fabric.resolve_workspace_name(id)
x
```

#### Get the current workspace ID
```python
import sempy
import sempy.fabric as fabric
x = fabric.get_workspace_id()
x
```

## Apps

#### Shows a list of apps within the workspace
```python
import sempy
import sempy.fabric as fabric
x = fabric.list_apps()
x
```

## Gateways

#### Shows a list of gateways within the workspace
```python
import sempy
import sempy.fabric as fabric
x = fabric.list_gateways()
x
```

## Reports

#### Shows a list of reports within the workspace
```python
import sempy
import sempy.fabric as fabric
x = fabric.list_reports()
x
```

#### Rebind a report to a dataset
* First run [this](https://github.com/m-kovalsky/Fabric/blob/main/ReportRebind.py) code to create the report_rebind function.
```python
reportName = '' #Enter the report name
datasetName = '' #Enter the dataset name
for item in report_list:
    report_rebind(reportName, datasetName)
```

#### Rebind a list of reports to a single dataset
* First run [this](https://github.com/m-kovalsky/Fabric/blob/main/ReportRebind.py) code to create the report_rebind function.
```python
report_list = ['', '', '', '', ''] #Enter your list of reports
datasetName = '' #Enter the dataset name
for item in report_list:
    report_rebind(item, datasetName)
```

#### Visualize a Power BI Report within a Fabric notebook
```python
import sempy
import sempy.fabric as fabric
from powerbiclient import Report

def launch_report(reportName):

    dfR = fabric.list_reports()
    filtered_df = dfR[(dfR['Name'] == reportName)]
    report_id = filtered_df['Id'].iloc[0]

    report = Report(group_id=None, report_id=report_id)
    return report

launch_report('') #Enter report name
```

## Dashboards

#### List dashboards in a given workspace
```python
import sempy
import sempy.fabric as fabric
import json
import pandas as pd

def list_dashboards(workspaceName=None):

    df = pd.DataFrame(columns=['Dashboard ID', 'Dashboard Name', 'Read Only', 'Web URL', 'Embed URL', 'Data Classification'])

    if workspaceName == 'None':
        groupId = fabric.get_workspace_id()
    else:
        groupId = fabric.resolve_workspace_id(workspaceName)

    client = fabric.PowerBIRestClient()
    response = client.get(f"/v1.0/myorg/groups/{groupId}/dashboards")
    responseJson = response.json()

    for v in responseJson['value']:
        dashboardID = v['id']
        displayName = v['displayName']
        isReadOnly = v['isReadOnly']
        webURL = v['webUrl']
        embedURL = v['embedUrl']
        dataClass = v['dataClassification']

        new_data = {'Dashboard ID': dashboardID, 'Dashboard Name': displayName, 'Read Only': isReadOnly, 'Web URL': webURL, 'Embed URL': embedURL, 'Data Classification': dataClass}
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True) 

    df['Read Only'] = df['Read Only'].astype(bool)

    return df

list_dashboards()
```

## Dataset and dataset objects

#### Shows a list of datasets in your current workspace
```python
import sempy
import sempy.fabric as fabric
x = fabric.list_datasets()
x
```

#### Shows a list of datasets in your current workspace with additional properties
```python
import sempy
import sempy.fabric as fabric
x = fabric.list_datasets(additional_xmla_properties=['Model.DefaultMode', 'Model.DirectLakeBehavior', 'CompatibilityLevel'])
x
```

#### Shows the dataset ID for a given dataset name
```python
import sempy
import sempy.fabric as fabric
datasetName = '' #Enter your dataset name
x = fabric.list_datasets()
x = x[x['Dataset Name'] == datasetName]
datasetID = x["Dataset ID"].values[0]
datasetID
```

#### Shows the [TMSL](https://learn.microsoft.com/analysis-services/tmsl/tabular-model-scripting-language-tmsl-reference?view=asallproducts-allversions) for a given dataset
```python
import sempy
import sempy.fabric as fabric
datasetName = '' #Enter your dataset name
workspaceName = '' #Enter your workspace name
x = fabric.get_tmsl(datasetName, workspaceName)
print(x)
```

#### List the data sources within a given dataset (semantic model)
```python
import sempy
import sempy.fabric as fabric
datasetName = '' #Enter dataset name
x = fabric.list_datasources(datasetName)
x
```

#### List the tables within a given dataset (semantic model)
```python
import sempy
import sempy.fabric as fabric
datasetName = '' #Enter dataset name
x = fabric.list_tables(datasetName)
x
```

#### List the columns within a given dataset (semantic model)
```python
import sempy
import sempy.fabric as fabric
datasetName = '' #Enter dataset name
x = fabric.list_columns(datasetName)
x
```

#### List the partitions within a given dataset (semantic model)
```python
import sempy
import sempy.fabric as fabric
datasetName = '' #Enter dataset name
x = fabric.list_partitions(datasetName)
x
```

#### List the partitions within a given dataset (semantic model) and properly format the Query column
```python
import sempy
import sempy.fabric as fabric
import pandas as pd

datasetName = '' #Enter dataset name
x = fabric.list_partitions(datasetName)

def format_sql_query(value):
    return value.replace('\n', '<br>').replace('\t', '&emsp;')

x_styled = x.style.format({'Query': format_sql_query})
x_styled
```

#### List the tables of a Direct Lake semantic model and show the lakehouse table from which it derives ('Query')
```python
import sempy
import sempy.fabric as fabric
datasetName = '' #Enter dataset name

dfP = fabric.list_partitions(datasetName)
dfP_filtered = dfP[dfP['Mode'] == 'DirectLake'][['Table Name', 'Query']]
dfP_filtered
```

#### List the measures within a given dataset (semantic model)
```python
import sempy
import sempy.fabric as fabric
datasetName = '' #Enter dataset name
x = fabric.list_measures(datasetName)
x
```

#### List the measures within a given dataset (semantic model) and properly format the Measure Expression column
```python
import sempy
import sempy.fabric as fabric
datasetName = '' #Enter dataset name
x = fabric.list_measures(datasetName)

def format_sql_query(value):
    return value.replace('\n', '<br>').replace('\t', '&emsp;')

x_styled = x.style.format({'Measure Expression': format_sql_query})
x_styled
```

#### List the hierarchies within a given dataset (semantic model)
```python
import sempy
import sempy.fabric as fabric
datasetName = '' #Enter dataset name
x = fabric.list_hierarchies(datasetName)
x
```

#### List the relationships within a given dataset (semantic model)
```python
import sempy
import sempy.fabric as fabric
datasetName = '' #Enter dataset name
x = fabric.list_relationships(datasetName)
x
```

#### Plot the relationships for a given dataset (semantic model)
```python
import sempy
import sempy.fabric as fabric
datasetName = '' #Enter dataset name
relationships = fabric.list_relationships(datasetName)
plot_relationship_metadata(relationships)
```

#### List the roles within a given dataset (semantic model)
```python
import sempy
import sempy.fabric as fabric
datasetName = '' #Enter dataset name
x = fabric.get_roles(datasetName)
x
```

#### List the roles and role members within a given dataset (semantic model)
```python
import sempy
import sempy.fabric as fabric
datasetName = '' #Enter dataset name
x = fabric.get_roles(dataset = datasetName, include_members = True)
x
```

#### List the row level security (RLS) for each role within a given dataset (semantic model)
```python
import sempy
import sempy.fabric as fabric
datasetName = '' #Enter dataset name
x = fabric.get_row_level_security_permissions(datasetName)
x
```

#### List the calculation groups and calculation items within a given dataset (semantic model)
```python
import sempy
import sempy.fabric as fabric
datasetName = '' #Enter dataset name
x = fabric.list_calculation_items(datasetName)
x
```

#### List the perspectives and perspective objects within a given dataset (semantic model)
```python
import sempy
import sempy.fabric as fabric
datasetName = '' #Enter dataset name
x = fabric.list_perspectives(datasetName)
x
```

#### List the translations within a given dataset (semantic model)
```python
import sempy
import sempy.fabric as fabric
datasetName = '' #Enter dataset name
x = fabric.list_translations(datasetName)
x
```

#### List the expressions (parameters) within a given dataset (semantic model)
```python
import sempy
import sempy.fabric as fabric
datasetName = '' #Enter dataset name
x = fabric.list_expressions(datasetName)
x
```

#### List the annotations within a given dataset (semantic model)
```python
import sempy
import sempy.fabric as fabric
datasetName = '' #Enter dataset name
x = fabric.list_annotations(datasetName)
x
```

#### Create a new blank semantic model
```python
import sempy
import sempy.fabric as fabric

def create_blank_semantic_model(datasetName, compatibilityLevel = 1604, workspaceName = None):

  if workspaceName == None:
    workspaceId = fabric.get_workspace_id()
    workspaceName = fabric.resolve_workspace_name(workspaceId)

  tmsl = f'''
  {{
    "createOrReplace": {{
      "object": {{
        "database": '{datasetName}'
      }},
      "database": {{
        "name": '{datasetName}',
        "compatibilityLevel": {compatibilityLevel},
        "model": {{
          "culture": "en-US",
          "defaultPowerBIDataSourceVersion": "powerBI_V3"
        }}
      }}
    }}
  }}
  '''

  return fabric.execute_tmsl(script = tmsl, workspace = workspaceName)

create_blank_semantic_model('') #Enter semantic model name to be created
```

## Dataset Refresh

Valid options for refresh_type: 'full', 'automatic', 'dataOnly', 'calculate', 'clearValues', 'defragment'. Default is 'automatic'.

#### Refresh a dataset
```python
import sempy
import sempy.fabric as fabric
datasetName = '' #Enter dataset name
fabric.refresh_dataset(dataset = datasetName, refresh_type = 'full')
```

#### Refresh specific table(s) in a dataset
```python
import sempy
import sempy.fabric as fabric
datasetName = '' #Enter dataset name
my_objects = [
    {"table": "tableName1"}, #Update 'tableName1' with your table name
    {"table": "tableName2"}  #Update 'tableName2' with your table name
]
fabric.refresh_dataset(dataset = datasetName, refresh_type = 'full', objects = my_objects)
```

#### Refresh specific partition(s) in a dataset
```python
import sempy
import sempy.fabric as fabric
datasetName = '' #Enter dataset name
my_objects = [
    {"table": "table1", "partition": "partition1"}, #Update 'table1' with your table name and 'partition1' with the partition name
    {"table": "table1", "partition": "partition2"}  #Update 'table1' with your table name and 'partition2' with the partition name
]
fabric.refresh_dataset(dataset = datasetName, refresh_type = 'full', objects = my_objects)
```

#### Refresh a combination of tables/partition(s) in a dataset
```python
import sempy
import sempy.fabric as fabric
datasetName = '' #Enter dataset name
my_objects = [
    {"table": "table1"}, #Update 'table1' with your table name
    {"table": "table2", "partition": "partition2"}  #Update 'table2' with your table name and 'partition2' with the partition name
]
fabric.refresh_dataset(dataset = datasetName, refresh_type = 'full', objects = my_objects)
```

#### Show refresh requests for a given dataset
```python
import sempy
import sempy.fabric as fabric
datasetName = '' #Enter dataset name
x = fabric.list_refresh_requests(datasetName)
x
```

## Read data from a dataset

#### Show a preview of the data in a given table from a dataset (semantic model)
```python
import sempy
import sempy.fabric as fabric
datasetName = '' #Enter dataset name
tableName = '' #Enter table name
rowLimit = 100
x = fabric.read_table(datasetName,tableName,False,rowLimit)
x
```

## Tabular Object Model

#### Connect to the [Tabular Object Model](https://learn.microsoft.com/analysis-services/tom/introduction-to-the-tabular-object-model-tom-in-analysis-services-amo?view=asallproducts-allversions) ([TOM](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.model?view=analysisservices-dotnet)); prints each table name
```python
import sempy
import sempy.fabric as fabric

workspaceName = '' #Enter workspace name
datasetName = '' #Enter dataset name

tom_server = fabric.create_tom_server(readonly=True, workspace=workspaceName)
m = tom_server.Databases.GetByName(datasetName).Model

for t in m.Tables:
    print(t.Name)
```

#### Create a new measure in a semantic model
```python
import sempy
import sempy.fabric as fabric
sempy.fabric._client._utils._init_analysis_services()
import Microsoft.AnalysisServices.Tabular as TOM

def create_measure(datasetName, tableName, measureName, measureExpression, workspaceName = None):

    if workspaceName == None:
        workspaceID = fabric.get_workspace_id()
        workspaceName = fabric.resolve_workspace_name(workspaceID)

    dfM = fabric.list_measures(dataset = datasetName, workspace = workspaceName)
    mExists = any(r['Measure Name'] == measureName for i, r in dfM.iterrows())
    if mExists:
        print(f"The '{measureName}' measure already exists in the '{datasetName}' semantic model within the '{workspaceName}' workspace.")
        return

    dfT = fabric.list_tables(dataset = datasetName, workspace = workspaceName)
    if not any(r['Name'] == tableName for i, r in dfT.iterrows()):
        print(f"The '{tableName}' table does not exist in the '{datasetName}' semantic model within the '{workspaceName}' workspace.")
        return

    tom_server = fabric.create_tom_server(readonly=False, workspace=workspaceName)
    m = tom_server.Databases.GetByName(datasetName).Model

    measure = TOM.Measure()
    measure.Name = measureName
    measure.Expression = measureExpression

    m.Tables[tableName].Measures.Add(measure)
    print(f"The '{measureName}' measure has been added to the '{tableName}' table in the '{datasetName} semantic model within the '{workspaceName}' workspace.")
    
    m.SaveChanges()
```

#### Update the DAX expression of an existing measure in a semantic model
```python
import sempy
import sempy.fabric as fabric
sempy.fabric._client._utils._init_analysis_services()
from sempy.fabric._cache import _get_or_create_workspace_client
from sempy.fabric._client._connection_mode import ConnectionMode
import Microsoft.AnalysisServices.Tabular as TOM

workspaceName = '' #Enter workspace name
datasetName = '' #Enter dataset name
tom_server = _get_or_create_workspace_client(workspaceName).get_dataset_client(datasetName, ConnectionMode.XMLA)._get_connected_dataset_server(readonly=False)

mName = '' #Enter measure name
mExpr = '' #Enter DAX expression
measure = TOM.Measure()
measure.Name = mName
measure.Expression = mExpr
for d in tom_server.Databases:
    if d.Name == datasetName:
        print(f"Updating {d.Name}...")
        m = d.Model
        for t in m.Tables:
            for ms in t.Measures:
                if ms.Name == mName:
                    ms.Expression = mExpr
                    print(f"The DAX expression for '{mName}' has been updated.")
        m.SaveChanges()
```

#### Example code to make sure you always get the latest model metadata
```python
import sempy
import sempy.fabric as fabric
from sempy.fabric._client import DatasetXmlaClient
from sempy.fabric._cache import _get_or_create_workspace_client
sempy.fabric._client._utils._init_analysis_services()
workspaceName = '' #Enter workspace name
datasetName = '' #Enter dataset name

tom_server = _get_or_create_workspace_client(workspaceName)._create_tom_server(False)
for d in tom_server.Databases:
    if d.Name == datasetName:
       m = d.Model
       for t in m.Tables:
         for ms in t.Measures:
            print(ms.Name + ' : ' + ms.Expression)

tom_server.Disconnect(True)
tom_server.Dispose()
```

## DAX

#### Run DAX via evaluate_dax()
```python
import sempy
import sempy.fabric as fabric
datasetName = '' #Enter dataset name
x = fabric.evaluate_dax(
    datasetName,
    """
    EVALUATE
    SUMMARIZECOLUMNS(
    "Header Name",[Measure Name] // update with actual measure name
    )
    """)
x
```

#### Run [Dynamic Management Views](https://learn.microsoft.com/analysis-services/instances/use-dynamic-management-views-dmvs-to-monitor-analysis-services?view=asallproducts-allversions) (DMVs) via evaluate_dax()
```python
import sempy
import sempy.fabric as fabric
datasetName = '' #Enter dataset name
x = fabric.evaluate_dax(
        datasetName,
        """
        SELECT
        MEASURE_GROUP_NAME AS [TableName]
        ,ATTRIBUTE_NAME AS [ColumnName]
        ,DATATYPE AS [DataType]
        ,DICTIONARY_SIZE AS [DictionarySize]
        ,DICTIONARY_ISRESIDENT AS [IsResident]
        ,DICTIONARY_TEMPERATURE AS [Temperature]
        ,DICTIONARY_LAST_ACCESSED AS [LastAccessed]
        FROM $SYSTEM.DISCOVER_STORAGE_TABLE_COLUMNS
        WHERE [COLUMN_TYPE] = 'BASIC_DATA'
        AND NOT [ISROWNUMBER]
        ORDER BY [DICTIONARY_TEMPERATURE] DESC
        """)
x
```

#### Examples using the new 'INFO' DAX functions
```python
import sempy
import sempy.fabric as fabric
datasetName = '' #Enter dataset name
x = fabric.evaluate_dax(
    datasetName,
    """
    // Select specific columns and rename columns      	
    EVALUATE
    SELECTCOLUMNS(INFO.TABLES(),"Table Name", [Name],"Hidden", [IsHidden],"Data Category", [DataCategory],[Description])    
    """)
x
```

```python
import sempy
import sempy.fabric as fabric
datasetName = '' #Enter dataset name
x = fabric.evaluate_dax(
    datasetName,
    """
    // Filter by multiple columns
    EVALUATE
    FILTER(INFO.TABLES(),[Name] = "TableName" && [IsHidden] = False) // Update filter with the actual table name
    """)
x
```

```python
import sempy
import sempy.fabric as fabric
datasetName = '' #Enter dataset name
x = fabric.evaluate_dax(
    datasetName,
    """
    // Select specific columns and filter
    EVALUATE
    FILTER(SELECTCOLUMNS(INFO.TABLES(),[Name],[IsHidden],[DataCategory],[Description]),[Name] = "TableName") // Update filter with the actual table name
    """)
x
```

```python
import sempy
import sempy.fabric as fabric
datasetName = '' #Enter dataset name
x = fabric.evaluate_dax(
    datasetName,
    """
    // View specific columns and order the table by a column in descending order
    EVALUATE
    SELECTCOLUMNS(COLUMNSTATISTICS(),"Table Name",[Table Name], [Column Name], [Cardinality])
    ORDER BY [Cardinality] DESC
    """)
x
```

#### Run a single measure against 1+ columns in your dataset
```python
import sempy
import sempy.fabric as fabric
x = fabric.evaluate_measure(
    "DatasetName", #Enter your dataset name
    "MeasureName", #Enter the measure name from your dataset   
    ["'TableName'[ColumnName]", "TableName[ColumnName]"]) # Enter columns
x
```

#### Enable DAX cell magic (ability to run DAX directly in a notebook cell using %%dax)
```python
%load_ext sempy
```

#### Run DAX using DAX cell magic
```python
%%dax "DatasetName" -w "WorkspaceName" # Enter DatasetName & WorkspaceName 

EVALUATE
SUMMARIZECOLUMNS(
"Header Name",[Measure Name] // Update with actual measure name
)
```

#### Run [Dynamic Management Views](https://learn.microsoft.com/analysis-services/instances/use-dynamic-management-views-dmvs-to-monitor-analysis-services?view=asallproducts-allversions) (DMVs) using DAX cell magic
```python
%%dax "DatasetName" -w "WorkspaceName" # Enter DatasetName & WorkspaceName 

SELECT * FROM $SYSTEM.DISCOVER_SESSIONS
```

## Vertipaq Info

#### Shows the current in memory model size based on the specific columns which are in memory
```python
import sempy
import sempy.fabric as fabric

datasetName = '' #Enter dataset name

dfC = fabric.list_columns(datasetName, extended=True)
dfC_filt = dfC[dfC['Is Resident']]
total_size_sum = dfC_filt['Total Size'].sum()
total_size_sum
```

## Direct Lake

#### View the columns which have the highest temperature and are in memory
```python
import sempy
import sempy.fabric as fabric

datasetName = '' #Enter dataset name

dfC = fabric.list_columns(datasetName, extended=True).sort_values(by='Temperature', ascending=False)
dfC
```

#### Show the guardrails by SKU for Direct Lake models
```python
import pandas as pd
url = 'https://learn.microsoft.com/power-bi/enterprise/directlake-overview'

tables = pd.read_html(url)
df = tables[0]
df['Fabric/Power BI SKUs'] = df['Fabric/Power BI SKUs'].str.split('/')
df = df.explode('Fabric/Power BI SKUs', ignore_index=True)
df
```

#### Show the Direct Lake guardrails for your workspace
```python
import sempy
import sempy.fabric as fabric
import pandas as pd

def get_sku_size(workspaceName = None):

    if workspaceName == None:
        workspaceId = fabric.get_workspace_id()
        workspaceName = fabric.resolve_workspace_name(workspaceId)

    dfC = fabric.list_capacities()
    dfW = fabric.list_workspaces().sort_values(by='Name', ascending=True)
    dfC.rename(columns={'Id': 'Capacity Id'}, inplace=True)
    dfCW = pd.merge(dfW, dfC[['Capacity Id', 'Sku', 'Region', 'State']], on='Capacity Id', how='inner')
    sku_value = dfCW.loc[dfCW['Name'] == workspaceName, 'Sku'].iloc[0]
    
    return sku_value

sku_value = get_sku_size()

def get_directlake_guardrails(skuSize):
    url = 'https://learn.microsoft.com/power-bi/enterprise/directlake-overview'

    tables = pd.read_html(url)
    df = tables[0]
    df['Fabric/Power BI SKUs'] = df['Fabric/Power BI SKUs'].str.split('/')
    df = df.explode('Fabric/Power BI SKUs', ignore_index=True)
    filtered_df = df[df['Fabric/Power BI SKUs'] == sku_value]
    
    return filtered_df

get_directlake_guardrails(sku_value)
```

## Migrate Power Query logic to Dataflows Gen2

Power Query logic can be migrated to Dataflows Gen2 by using a [Power Query Template](https://learn.microsoft.com/power-query/power-query-template) file and then importing that file into Dataflows Gen2. This will migrate all of your tables to Dataflows Gen2 in one swoop.

The following process dynamically creates the Power Query Template (.pqt) file so you can import it into Dataflows Gen2 to create delta tables in your Fabric lakehouse.

*Note: For multi-partitioned tables, this process will take just the first partition from the table. In the case of Incremental Refresh, it will take the Source Expression (M query) from the table and ignore the individual partitions.*

### [Fabric Notebook](https://learn.microsoft.com/fabric/data-engineering/how-to-use-notebook)
1. Connect the notebook to your lakehouse.
2. Run the following command in a Fabric notebook to install the .whl file.
```python
%pip install "%pip install "https://raw.githubusercontent.com/m-kovalsky/fabric_cat_tools/main/fabric_cat_tools-0.2.4-py3-none-any.whl""
```
3. Run this code, specifying the semantic model name in the datasetName parameter.
```python
import fabric_cat_tools as fct
datasetName = '' #Enter the import/DQ semantic model name
fct.create_pqt_file(datasetName)
```
4. Make sure that you have installed [OneLake file explorer](https://www.microsoft.com/download/details.aspx?id=105222).
5. Create a new Dataflow Gen2 within your Fabric workspace.
6. Select the PowerQueryTemplate.pqt file created in step 3 (*note: you may have to right click on the folder in the Windows file explorer and select 'OneLake -> Sync from OneLake'*).
7. Click 'Configure connection' to configure the connection to the data source.
8. Select a destination for each table (your desired lakehouse).
9. Click 'Publish'.
 
## Direct Lake migration

The documentation for Direct Lake migration has been moved [here](https://github.com/m-kovalsky/fabric_cat_tools?tab=readme-ov-file#direct-lake-migration-1).
