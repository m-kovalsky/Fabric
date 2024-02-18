import sempy
import sempy.fabric as fabric
import json
import pandas as pd

def list_dataflow_storage_accounts():
    
    df = pd.DataFrame(columns=['Dataflow Storage Account ID', 'Dataflow Storage Account Name', 'Enabled'])
    client = fabric.PowerBIRestClient()
    response = client.get(f"/v1.0/myorg/dataflowStorageAccounts")
    
    for v in response.json()['value']:
        dfsaId = v['id']
        dfsaName = v['name']
        isEnabled = v['isEnabled']
        
        new_data = {'Dataflow Storage Account ID': dfsaId, 'Dataflow Storage Account Name': dfsaName, 'Enabled': isEnabled}
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    df['Enabled'] = df['Enabled'].astype(bool)

    return df