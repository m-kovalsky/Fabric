import notebookutils
import requests

key_vault_uri = ''
secret_name = ''
token = notebookutils.credentials.getToken("keyvault")
headers = {"Authorization": f"Bearer {token}"}
response = requests.get(
    f"{key_vault_uri}/secrets/{secret_name}?api-version=2025-07-01", headers=headers,
)
url = response.json().get("value")[-1].get("id")
response = requests.get(f"{url}?api-version=2025-07-01")
value = response.json().get("value")
print(value)
