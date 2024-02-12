import sempy
import sempy.fabric as fabric
import json, requests, pandas as pd

def report_rebind(reportName, datasetName):

    key_vault = 'https://mykeyvault.vault.azure.net/' # Enter your key vault
    tenant = mssparkutils.credentials.getSecret(key_vault , 'TenantId') # Enter your Tenant Id parameter
    client = mssparkutils.credentials.getSecret(key_vault , 'ClientId') # Enter your Client (Application) Id parameter
    client_secret = mssparkutils.credentials.getSecret(key_vault , 'ClientSecret') # Enter your Client Secret parameter

    # Get access token
    try:
        from azure.identity import ClientSecretCredential
    except Exception:
        !pip install azure.identity
        from azure.identity import ClientSecretCredential
    
    api = 'https://analysis.windows.net/powerbi/api/.default'
    auth = ClientSecretCredential(authority = 'https://login.microsoftonline.com/',
                                tenant_id = tenant,
                                client_id = client,
                                client_secret = client_secret)
    access_token = auth.get_token(api)
    access_token = access_token.token
    
    print('\nSuccessfully authenticated.')

    groupId = fabric.get_workspace_id()
    itemList = fabric.list_items()

    # Get report ID
    itemListFilt = itemList[(itemList['Display Name'] == reportName) & (itemList['Type'] == 'Report')]
    reportId = itemListFilt['Id'].iloc[0]

    # Get dataset ID for rebinding
    itemListFilt = itemList[(itemList['Display Name'] == datasetName) & (itemList['Type'] == 'SemanticModel')]
    datasetId = itemListFilt['Id'].iloc[0]

    # Prepare API
    api_url = "https://api.powerbi.com/v1.0/myorg/groups/{groupId}/reports/{reportId}/Rebind"
    header = {'Authorization': f'Bearer {access_token}'}
    request_body = {
        'datasetId': datasetId
    }

    response = requests.post(api_url.format(groupId=groupId, reportId=reportId), json=request_body, headers=header)

    if response.status_code == 200:
        print('POST request successful')
        print(f"Report '{reportName}' has been successfully rebinded to the '{datasetName}' dataset.")
    else:
        print(f"POST request failed with status code: {response.status_code}")

report_rebind('FramingTest', 'Frame') # Enter report name you want to rebind, enter the dataset to which you want to rebind the report