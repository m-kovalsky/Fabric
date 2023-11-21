# Fabric
The following code snippets may be executed in a [Microsoft Fabric notebook](https://learn.microsoft.com/fabric/data-engineering/how-to-use-notebook). These scripts leverage the Semantic Link (a.k.a. Sempy) library.

This repo is intended to help business intelligence analysts/developers and data scientists become more familiar with notebooks (and therefore Python) in Microsoft Fabric and the potential benefits of using the Semantic Link library.

#### Load the [Semantic-Link](https://pypi.org/project/semantic-link/) library inside of your notebook
```python
%pip install semantic-link
```

## Loading Semantic-Link into a custom Fabric environment
An even better way to ensure the Semantic-Link library is available in your workspace/notebooks is to load it as a library in ac custom Fabric environment. If you do this, you will not have to run the above '%pip install semantic-link' code every time in your notebook. Please follow the steps below to create an environment and load the Semantic-Link library.

#### Create a custom environment
1. Navigate to your Fabric workspace
2. Click 'New' -> More options
3. Within 'Data Science', click 'Environment'
4. Name your environment, click 'Create'

#### Add Semantic-Link as a library to the environment
1. Within 'Public Libraries', click 'Add from PyPI'
2. Enter 'Semantic-Link' in the text box under 'Library'
3. Click 'Save' at the top right of the screen
4. Click 'Publish' at the top right of the screen
5. Click 'Publish All'

#### Update your notebook to use the new environment (*must wait for the environment to finish publishing*)
1. Navigate to your Notebook
2. Select your newly created environment within the 'Environment' drop down in the navigation bar at the top of the notebook

## Packages, functions and Sempy help

#### Useful packages for python notebooks (Sempy and more)
```python
import sempy
import sempy.fabric as fabric
import pandas as pd
import pyspark.sql.functions as F
```

### Show the directory of all sempy functions
```python
import sempy.fabric
dir(sempy.fabric)
```

#### Show useful information about a given function
```python
import sempy.fabric as fabric
help(fabric.list_datasets) #Replace 'list_datasets' within any function shown in the dir(sempy.fabric) output
```

## Workspace/Lakehouse objects

#### Gets the Lakehouse ID from the current lakehouse
```python
import sempy.fabric as fabric
x = fabric.get_lakehouse_id()
x
```

#### Gets the Artifact ID (of the notebook)
```python
import sempy.fabric as fabric
x = fabric.get_artifact_id()
x
```

#### Shows a list of your accessible workspaces, sorted alphabetically
```python
import sempy.fabric as fabric
x = fabric._list_workspaces().sort_values(by='Name', ascending=True)
x
```

#### Filter to a particular workspace
```python
import sempy.fabric as fabric
workspaceName = "" #Enter the workspace name to be used as a filter
x = fabric._list_workspaces()
filter_condition = [workspaceName]
x = x[x['Name'].isin(filter_condition)]
x
```

#### Filter to a particular workspace and extract the value
```python
import sempy.fabric as fabric
workspaceName = "" #Enter the workspace name to be used as a filter
x = fabric._list_workspaces()
filter_condition = [workspaceName]
x = x[x['Name'].isin(filter_condition)]
y = x["Id"].values[0]
z = fabric.resolve_workspace_name(y)
z
```

#### Find the workspace ID for a given workspace name
```python
import sempy.fabric as fabric
x = "" #Enter the workspace name
id = fabric.resolve_workspace_id(x)
id
```

#### Find the workspace name for a given workspace ID
```python
import sempy.fabric as fabric
id = "" #Enter the workspace ID
x = fabric.resolve_workspace_name(workspaceID)
x
```

#### Get the current workspace ID
```python
import sempy.fabric as fabric
x = fabric.get_workspace_id()
x
```

## Dataset and dataset objects

#### Shows a list of datasets in your current workspace
```python
import sempy.fabric as fabric
x = fabric.list_datasets()
x
```

#### Shows the [TMSL](https://learn.microsoft.com/analysis-services/tmsl/tabular-model-scripting-language-tmsl-reference?view=asallproducts-allversions) for a given dataset
```python
import sempy.fabric as fabric
datasetName = "" #Enter your dataset name
workspaceName = "" #Enter your workspace name
x = fabric.get_tmsl(datasetName, workspaceName)
print(x)
```

#### List the tables within a given dataset (semantic model)
```python
import sempy.fabric as fabric
datasetName = "" #Enter dataset name
x = fabric.list_tables(datasetName)
x
```

#### List the columns within a given dataset (semantic model)
```python
import sempy.fabric as fabric
datasetName = "" #Enter dataset name
x = fabric.list_columns(datasetName)
x
```

#### List the partitions within a given dataset (semantic model)
```python
import sempy.fabric as fabric
datasetName = "" #Enter dataset name
x = fabric.list_partitions(datasetName)
x
```

#### List the measures within a given dataset (semantic model)
```python
import sempy.fabric as fabric
datasetName = "" #Enter dataset name
x = fabric.list_measures(datasetName)
x
```

#### List the hierarchies within a given dataset (semantic model)
```python
import sempy.fabric as fabric
datasetName = "" #Enter dataset name
x = fabric.list_hierarchies(datasetName)
x
```

#### List the relationships within a given dataset (semantic model)
```python
import sempy.fabric as fabric
datasetName = "" #Enter dataset name
x = fabric.list_relationships(datasetName)
x
```

#### Plot the relationships for a given dataset (semantic model)
```python
import sempy.fabric as fabric
datasetName = "" #Enter dataset name
relationships = fabric.list_relationships(datasetName)
plot_relationship_metadata(relationships)
```

#### List the translations within a given dataset (semantic model)
```python
import sempy.fabric as fabric
datasetName = "" #Enter dataset name
x = fabric.list_translations(datasetName)
x

#### List the expressions (parameters) within a given dataset (semantic model)
```python
import sempy.fabric as fabric
datasetName = "" #Enter dataset name
x = fabric.list_expressions(datasetName)
x

## Dataset Refresh

#### Refresh a dataset
```python
import sempy.fabric as fabric
datasetName = "" #Enter dataset name
fabric.refresh_dataset("Covid dataset 3",refresh_type = "full")
```

## Read data from dataset

#### Show a preview of the data in a given table from a dataset (semantic model)
```python
import sempy.fabric as fabric
datasetName = "" #Enter dataset name
tableName = "" #Enter table name
rowLimit = 100
x = fabric.read_table(datasetName,tableName,False,rowLimit)
x
```
## Tabular Object Model

#### Connect to the [Tabular Object Model](https://learn.microsoft.com/analysis-services/tom/introduction-to-the-tabular-object-model-tom-in-analysis-services-amo?view=asallproducts-allversions) ([TOM](https://learn.microsoft.com/dotnet/api/microsoft.analysisservices.tabular.model?view=analysisservices-dotnet)); prints each table name
```python
import sempy.fabric as fabric
from sempy.fabric._client import DatasetXmlaClient
from sempy.fabric._cache import _get_or_create_workspace_client
sempy.fabric._client._utils._init_analysis_services()
workspaceName = "" #Enter workspace name
datasetName = "" #Enter dataset name
workspace_client = _get_or_create_workspace_client(workspaceName)
ds = workspace_client.get_dataset(datasetName)

m = ds.Model

for t in m.Tables:
	print(t.Name)
```

## DAX

#### Run DAX via evaluate_dax()
```python
import sempy.fabric as fabric
datasetName = "" #Enter dataset name
x = fabric.evaluate_dax(
    datasetName,
    """
    EVALUATE
    SUMMARIZECOLUMNS(
    "Header Name",[Measure Name]
    )
    """
    )
x
```

#### Run [Dynamic Management Views](https://learn.microsoft.com/analysis-services/instances/use-dynamic-management-views-dmvs-to-monitor-analysis-services?view=asallproducts-allversions) (DMVs) via evaluate_dax()
```python
import sempy.fabric as fabric
datasetName = "" #Enter dataset name
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

#### Run a single measure against 1+ columns in your dataset
```python
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
"Header Name",[Measure Name]
)
```

#### Run [Dynamic Management Views](https://learn.microsoft.com/analysis-services/instances/use-dynamic-management-views-dmvs-to-monitor-analysis-services?view=asallproducts-allversions) (DMVs) using DAX cell magic
```python
%%dax "DatasetName" -w "WorkspaceName" # Enter DatasetName & WorkspaceName 

SELECT * FROM $SYSTEM.DISCOVER_SESSIONS
```
