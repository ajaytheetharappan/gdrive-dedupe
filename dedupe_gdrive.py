#!/usr/bin/env python3
"""
Deduplicates files in a Google Drive folder by content hash,
copying unique files to a destination folder (default name: "y").
"""

import hashlib
import io
import sys
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

SCOPES = ["https://www.googleapis.com/auth/drive"]
CREDS_FILE = Path(__file__).parent / "credentials.json"
TOKEN_FILE = Path(__file__).parent / "token.json"


def get_service():
    creds = None
    if TOKEN_FILE.exists():
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        TOKEN_FILE.write_text(creds.to_json())
    return build("drive", "v3", credentials=creds)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def list_files(service, folder_id: str) -> list[dict]:
    results = []
    page_token = None
    while True:
        resp = service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="nextPageToken, files(id, name, mimeType)",
            pageToken=page_token,
        ).execute()
        results.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return results


def download_file(service, file_id: str) -> bytes:
    request = service.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buf.getvalue()


def get_or_create_folder(service, name: str, parent_id: str | None = None) -> str:
    query = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    resp = service.files().list(q=query, fields="files(id, name)").execute()
    files = resp.get("files", [])
    if files:
        return files[0]["id"]
    metadata = {"name": name, "mimeType": "application/vnd.google-apps.folder"}
    if parent_id:
        metadata["parents"] = [parent_id]
    folder = service.files().create(body=metadata, fields="id").execute()
    return folder["id"]


def upload_file(service, name: str, data: bytes, mime_type: str, parent_id: str) -> None:
    metadata = {"name": name, "parents": [parent_id]}
    media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mime_type)
    service.files().create(body=metadata, media_body=media, fields="id").execute()


def dedupe(src_folder_id: str, dst_folder_name: str = "y") -> None:
    service = get_service()

    print(f"Listing files in source folder {src_folder_id}...")
    files = list_files(service, src_folder_id)

    # Skip Google Docs native files (can't be downloaded with get_media)
    files = [f for f in files if not f["mimeType"].startswith("application/vnd.google-apps")]

    if not files:
        print("No downloadable files found.")
        return

    # Get the parent of the source folder to create destination alongside it
    src_meta = service.files().get(fileId=src_folder_id, fields="parents").execute()
    parent_id = src_meta.get("parents", [None])[0]

    print(f"Creating/finding destination folder '{dst_folder_name}'...")
    dst_folder_id = get_or_create_folder(service, dst_folder_name, parent_id)

    seen: dict[str, str] = {}  # hash -> original filename
    duplicates = []

    for f in files:
        print(f"  Downloading  {f['name']}...", end=" ", flush=True)
        data = download_file(service, f["id"])
        digest = sha256_bytes(data)

        if digest in seen:
            duplicates.append((f["name"], seen[digest]))
            print(f"DUPLICATE of {seen[digest]}")
        else:
            seen[digest] = f["name"]
            ext = Path(f["name"]).suffix
            hashed_name = digest + ext
            upload_file(service, hashed_name, data, f["mimeType"], dst_folder_id)
            print(f"-> {hashed_name}")

    print(f"\nTotal files : {len(files)}")
    print(f"Unique files: {len(seen)}")
    print(f"Duplicates  : {len(duplicates)}")
    if duplicates:
        print("\nDuplicates found:")
        for dup, orig in duplicates:
            print(f"  {dup}  ==  {orig}")
    print(f"\nUnique files uploaded to Drive folder '{dst_folder_name}' (id: {dst_folder_id})")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 dedupe_gdrive.py <source_folder_id> [dest_folder_name]")
        print("\nTo get a folder ID: open the folder in Google Drive,")
        print("the ID is the last part of the URL:")
        print("  https://drive.google.com/drive/folders/<FOLDER_ID>")
        sys.exit(1)

    src_id = sys.argv[1]
    dst_name = sys.argv[2] if len(sys.argv) > 2 else "y"
    dedupe(src_id, dst_name)
