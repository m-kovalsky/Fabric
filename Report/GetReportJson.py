import sempy
import sempy.fabric as fabric
import pandas as pd
import base64
import json

client = fabric.FabricRestClient()

workspaceId = fabric.get_workspace_id()
objectName = "" #Enter report name
objectType = "Report"
itemList = fabric.list_items()
itemListFilt = itemList[(itemList['Display Name'] == objectName) & (itemList['Type'] == objectType)]
itemId = itemListFilt['Id'].iloc[0]
response = client.post(f"/v1/workspaces/{workspaceId}/items/{itemId}/getDefinition")
df_items = pd.json_normalize(response.json()['definition']['parts'])
df_items_filt = df_items[df_items['path'] == 'report.json']
payload = df_items_filt['payload'].iloc[0]

reportFile = base64.b64decode(payload).decode('utf-8')
reportJson = json.loads(reportFile)
reportJson