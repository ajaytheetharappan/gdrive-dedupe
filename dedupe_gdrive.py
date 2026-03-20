#!/usr/bin/env python3
"""
Downloads a zip from a Drive folder, extracts it locally, then compares
the extracted files against an existing Drive folder by content hash.
New files (not already present by hash) are uploaded to an output folder.

Usage:
  python3 dedupe_gdrive.py <zip_folder_id> <existing_folder_id> <output_folder_id>
"""

import hashlib
import io
import sys
import tempfile
import zipfile
import mimetypes
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


def upload_file(service, name: str, data: bytes, mime_type: str, parent_id: str) -> None:
    metadata = {"name": name, "parents": [parent_id]}
    media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mime_type)
    service.files().create(body=metadata, media_body=media, fields="id").execute()


def get_zip_from_folder(service, folder_id: str) -> tuple[str, bytes]:
    """Find and download the first zip file in a Drive folder."""
    files = list_files(service, folder_id)
    zips = [f for f in files if f["name"].endswith(".zip")]
    if not zips:
        print("No zip file found in the zip folder.")
        sys.exit(1)
    zip_file = zips[0]
    print(f"Found zip: {zip_file['name']}")
    print("Downloading zip...", end=" ", flush=True)
    data = download_file(service, zip_file["id"])
    print("done.")
    return zip_file["name"], data


def extract_zip(zip_data: bytes, extract_dir: Path) -> list[Path]:
    """Extract zip to a local directory, return list of extracted file paths."""
    with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
        zf.extractall(extract_dir)
    # Return all files recursively, skip hidden/system files
    return [p for p in extract_dir.rglob("*") if p.is_file() and not p.name.startswith(".")]


def build_hash_set(service, folder_id: str) -> set[str]:
    """Download all files in a Drive folder and return their SHA-256 hashes."""
    print(f"Building hash index of existing folder...")
    files = list_files(service, folder_id)
    files = [f for f in files if not f["mimeType"].startswith("application/vnd.google-apps")]
    hashes = set()
    for f in files:
        print(f"  Hashing  {f['name']}...", end=" ", flush=True)
        data = download_file(service, f["id"])
        hashes.add(sha256_bytes(data))
        print("done.")
    print(f"Existing files indexed: {len(hashes)}\n")
    return hashes


def run(zip_folder_id: str, existing_folder_id: str, output_folder_id: str) -> None:
    service = get_service()

    # Step 1: Download and extract the zip
    _, zip_data = get_zip_from_folder(service, zip_folder_id)
    with tempfile.TemporaryDirectory() as tmpdir:
        extract_dir = Path(tmpdir)
        print("Extracting zip...", end=" ", flush=True)
        new_files = extract_zip(zip_data, extract_dir)
        print(f"done. {len(new_files)} files extracted.\n")

        # Step 2: Build hash index of existing Drive folder
        existing_hashes = build_hash_set(service, existing_folder_id)

        # Step 3: Compare and upload new files
        print("Comparing new files against existing...")
        uploaded = 0
        skipped = 0
        for path in sorted(new_files):
            data = path.read_bytes()
            digest = sha256_bytes(data)
            if digest in existing_hashes:
                print(f"  SKIP      {path.name}  (already exists)")
                skipped += 1
            else:
                mime_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
                print(f"  UPLOAD    {path.name}...", end=" ", flush=True)
                upload_file(service, path.name, data, mime_type, output_folder_id)
                existing_hashes.add(digest)
                print("done.")
                uploaded += 1

        print(f"\nTotal new files : {len(new_files)}")
        print(f"Uploaded        : {uploaded}")
        print(f"Skipped (dupes) : {skipped}")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python3 dedupe_gdrive.py <zip_folder_id> <existing_folder_id> <output_folder_id>")
        print()
        print("  zip_folder_id      — Drive folder containing the zip of new files")
        print("  existing_folder_id — Drive folder with existing files to compare against")
        print("  output_folder_id   — Drive folder where new unique files will be uploaded")
        sys.exit(1)

    run(sys.argv[1], sys.argv[2], sys.argv[3])
