import sempy
import sempy.fabric as fabric
from sempy.fabric._client import DatasetXmlaClient
from sempy.fabric._cache import _get_or_create_workspace_client
sempy.fabric._client._utils._init_analysis_services()
workspaceName = "" #Enter workspace name
datasetName = "" #Enter dataset name
workspace_client = _get_or_create_workspace_client(workspaceName)
ds = workspace_client.get_dataset(datasetName)

m = ds.Model

isDirectLake = "No"

for t in m.Tables:
    for p in t.Partitions:
        if str(p.Mode) == "DirectLake":
            isDirectLake = "Yes"

if isDirectLake == "No":
    print(isDirectLake + ", this model is not in DirectLake mode")
else:
    print(isDirectLake + ", this model is in DirectLake mode")

if isDirectLake == "Yes":
    for e in m.Expressions:
        index = e.Expression.find("(\"") + 2
        index2 = e.Expression.find(".net\"") + 4
        sqlendpoint = e.Expression[index:index2]
        print("SQL Endpoint: " + sqlendpoint)
        index3 = e.Expression.find(",") + 3
        index4 = e.Expression.find("\")")
        lakehouseName = e.Expression[index3:index4]
        print("Lakehouse: " + lakehouseName)