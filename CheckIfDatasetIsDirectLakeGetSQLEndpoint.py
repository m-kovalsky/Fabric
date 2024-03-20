import sempy
import sempy.fabric as fabric
import re

workspaceName = '' #Enter workspace name
datasetName = '' #Enter dataset name

dfP = fabric.list_partitions(dataset = datasetName, workspace = workspaceName)
isDirectLake = any(r['Mode'] == 'DirectLake' for i, r in dfP.iterrows())

if isDirectLake:
    print(f"The '{datasetName}' semantic model in the '{workspaceName}' workspace is in Direct Lake mode.")
    dfE = fabric.list_expressions(dataset = datasetName, workspace = workspaceName)
    dfE_filt = dfE[dfE['Name'] == 'DatabaseQuery']
    expr = dfE_filt['Expression'].iloc[0]
    
    matches = re.findall(r'"([^"]*)"', expr)
    sqlEndpoint = matches[0]
    sqlEndpointId = matches[1]

    print('SQL Endpoint: ' + sqlEndpoint)
    print('SQL Endpoint ID: ' + sqlEndpointId)
else:
    print(f"The '{datasetName}' semantic model in the '{workspaceName}' workspace is not in Direct Lake mode.")
