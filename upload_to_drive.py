import json
import os
import sys
from pathlib import Path

script_dir = Path(__file__).parent
creds_path = script_dir / "credentials.json"
tokens_path = script_dir / "tokens.json"

with open(creds_path) as f:
    creds_data = json.load(f)

with open(tokens_path) as f:
    token_data = json.load(f)

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

creds_info = creds_data.get("installed") or creds_data.get("web")
client_id = creds_info["client_id"]
client_secret = creds_info["client_secret"]
token_uri = creds_info.get("token_uri", "https://oauth2.googleapis.com/token")

creds = Credentials(
    token=token_data.get("access_token") or token_data.get("token"),
    refresh_token=token_data.get("refresh_token"),
    token_uri=token_uri,
    client_id=client_id,
    client_secret=client_secret,
    scopes=token_data.get("scopes") or token_data.get("scope", "").split(),
)

service = build("drive", "v3", credentials=creds)

results = service.files().list(
    q="name='Byte Orange' and mimeType='application/vnd.google-apps.folder' and trashed=false",
    fields="files(id, name)",
    spaces="drive"
).execute()

folders = results.get("files", [])
if not folders:
    print("ERROR: Could not find folder 'Byte Orange' in Drive.")
    sys.exit(1)

folder = folders[0]
folder_id = folder["id"]
print(f"Found folder: {folder['name']} (id={folder_id})")

csv_path = r"C:\Users\Kush\AppData\Roaming\Claude\local-agent-mode-sessions\a21f61e7-e111-40d5-aadf-b2f83c51381e\d0f948e8-f218-4659-bed8-0c78c2c334e9\agent\local_ditto_d0f948e8-f218-4659-bed8-0c78c2c334e9\uploads\019d8132-crypto_leads_filtered.csv"
file_name = os.path.basename(csv_path)

file_metadata = {"name": file_name, "parents": [folder_id]}
media = MediaFileUpload(csv_path, mimetype="text/csv", resumable=True)
uploaded = service.files().create(
    body=file_metadata,
    media_body=media,
    fields="id, name, webViewLink"
).execute()

file_id = uploaded["id"]
file_link = uploaded.get("webViewLink", f"https://drive.google.com/file/d/{file_id}/view")

print(f"\nUploaded: {uploaded['name']}")
print(f"File ID: {file_id}")
print(f"Drive link: {file_link}")
