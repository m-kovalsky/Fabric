import sempy
import sempy.fabric as fabric
import json
import pandas as pd
import base64

# Make sure to run the patch_dataset_datasource function after running this script in order for the data source credentials to populate properly

def create_semantic_model(datasetName, bimFile, workspaceName = None):

    objectType = 'SemanticModel'
    client = fabric.FabricRestClient()
    if workspaceName == None:
        workspaceId = fabric.get_workspace_id()
        workspaceName = fabric.resolve_workspace_name(workspaceId)
    else:
        fabric.resolve_workspace_id(workspaceName)

    defPBIDataset = {
    "version": "1.0",
    "settings": {}
    }

    def conv_b64(file):
        
        loadJson = json.dumps(file)
        f = base64.b64encode(loadJson.encode('utf-8')).decode('utf-8')
        
        return f

    payloadPBIDefinition = conv_b64(defPBIDataset)
    payloadBim = conv_b64(bim)

    request_body = {
            'displayName': datasetName,
            'type': objectType,
            'definition': {
        "parts": [
            {
                "path": "model.bim",
                "payload": payloadBim,
                "payloadType": "InlineBase64"
            },
            {
                "path": "definition.pbidataset",
                "payload": payloadPBIDefinition,
                "payloadType": "InlineBase64"
            }
        ]

            }
        }

    response = client.post(f"/v1/workspaces/{workspaceId}/items",json=request_body)

    if response.status_code == 201:
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
        print('Model creation succeeded')
        print(response.json())