import sempy
import sempy.fabric as fabric
import json
import time
import base64
import pandas as pd

# This script gets the model.bim file for a given dataset (semantic model)

def get_datset_bim(datasetName):

    objType = 'SemanticModel'
    client = fabric.FabricRestClient()
    workspaceId = fabric.get_workspace_id()
    itemList = fabric.list_items()
    itemListFilt = itemList[(itemList['Display Name'] == datasetName) & (itemList['Type'] == objType)]
    itemId = itemListFilt['Id'].iloc[0]
    response = client.post(f"/v1/workspaces/{workspaceId}/items/{itemId}/getDefinition")
        
    if response.status_code == 200:
        print(response.json())
    elif response.status_code == 202:
        location_url = response.headers['Location']
        operationId = response.headers['x-ms-operation-id']
        response = client.get(f"/v1/operations/{operationId}")
        response_body = json.loads(response.content) 
        while response_body['status'] != 'Succeeded':
            time.sleep(3)
            response = client.get(f"/v1/operations/{operationId}")
            response_body = json.loads(response.content)
        response = client.get(f"/v1/operations/{operationId}/result")
        df_items = pd.json_normalize(response.json()['definition']['parts'])
        df_items_filt = df_items[df_items['path'] == 'model.bim']
        payload = df_items_filt['payload'].iloc[0]
        bimFile = base64.b64decode(payload).decode('utf-8')
        bimJson = json.loads(bimFile)

        return bimJson