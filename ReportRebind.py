import sempy
import sempy.fabric as fabric
import json, requests, pandas as pd

# reportName: this is the report you want to rebind the dataset
# datasetName: this is the dataset to which you want to rebind the report

def report_rebind(reportName, datasetName):

    client = fabric.PowerBIRestClient()
    groupId = fabric.get_workspace_id()
    itemList = fabric.list_items()

    # Get report ID
    itemListFilt = itemList[(itemList['Display Name'] == reportName) & (itemList['Type'] == 'Report')]
    reportId = itemListFilt['Id'].iloc[0]

    # Get dataset ID for rebinding
    itemListFilt = itemList[(itemList['Display Name'] == datasetName) & (itemList['Type'] == 'SemanticModel')]
    datasetId = itemListFilt['Id'].iloc[0]

    # Prepare API
    request_body = {
        'datasetId': datasetId
    }

    response = client.post(f"/v1.0/myorg/groups/{groupId}/reports/{reportId}/Rebind",json=request_body)

    if response.status_code == 200:
        print('POST request successful')
        print(f"Report '{reportName}' has been successfully rebinded to the '{datasetName}' dataset.")
    else:
        print(f"POST request failed with status code: {response.status_code}")