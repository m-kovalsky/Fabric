import sempy
import sempy.fabric as fabric
import json
import pandas as pd

# Run this script after running the create_semantic_model script

def patch_dataset_datasource(datasetName):

    aadFlag = 'False'
    dsInfo = get_dataset_datasource(datasetName)
    gatewayId = dsInfo['Gateway ID'].iloc[0]
    datasourceId = dsInfo['Datasource ID'].iloc[0]

    gDS = list_gateway_datasource(gatewayId)
    gDS_filt = gDS[gDS['Data Source ID'] == datasourceId]
    credType = gDS_filt['Credential Type'].iloc[0]

    if credType == 'OAuth2':
        aadFlag = 'True'

    client = fabric.PowerBIRestClient()
    request_body = {
    "credentialDetails": {
        "credentialType": credType,
        "encryptedConnection": "Encrypted",
        "encryptionAlgorithm": "None",
        "privacyLevel": "Organizational",
        "useCallerAADIdentity": aadFlag
    }
    }

    response = client.patch(f"/v1.0/myorg/gateways/{gatewayId}/datasources/{datasourceId}", json=request_body)

    return response