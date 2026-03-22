#!/usr/bin/env python3
"""
Downloads a zip from a Drive folder, extracts it locally, then compares
the extracted files against an output Drive folder by content hash.
New files (not already present by hash) are uploaded to the output folder.

A hashes.json cache file is stored in the output folder to speed up
subsequent runs (avoids re-downloading all files to recompute hashes).

Usage:
  python3 dedupe_gdrive.py <zip_folder_id> <output_folder_id>
"""

import hashlib
import io
import json
import sys
import tempfile
import time
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
HASH_CACHE_NAME = "hashes.json"


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
    def _download():
        request = service.files().get_media(fileId=file_id)
        buf = io.BytesIO()
        downloader = MediaIoBaseDownload(buf, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return buf.getvalue()
    return with_retry(_download)


def with_retry(fn, retries: int = 5, backoff: float = 2.0):
    """Call fn(), retrying on network/timeout errors with exponential backoff."""
    for attempt in range(retries):
        try:
            return fn()
        except Exception as e:
            if attempt == retries - 1:
                raise
            wait = backoff ** attempt
            print(f"  [retry {attempt + 1}/{retries - 1}] {e} — retrying in {wait:.0f}s...")
            time.sleep(wait)


def upload_file(service, name: str, data: bytes, mime_type: str, parent_id: str) -> str:
    """Upload a file and return its Drive file ID."""
    def _upload():
        metadata = {"name": name, "parents": [parent_id]}
        media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mime_type)
        f = service.files().create(body=metadata, media_body=media, fields="id").execute()
        return f["id"]
    return with_retry(_upload)


def update_file(service, file_id: str, data: bytes, mime_type: str) -> None:
    """Update an existing Drive file's content in place."""
    def _update():
        media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mime_type)
        service.files().update(fileId=file_id, media_body=media).execute()
    with_retry(_update)


def get_zips_from_folder(service, folder_id: str) -> list[dict]:
    files = list_files(service, folder_id)
    zips = [f for f in files if f["name"].endswith(".zip") or f["mimeType"] == "application/zip"]
    if not zips:
        print("No zip files found in the input folder.")
        sys.exit(1)
    print(f"Found {len(zips)} zip(s):")
    for z in zips:
        print(f"  - {z['name']}")
    return zips


def extract_zip(zip_data: bytes, extract_dir: Path) -> list[Path]:
    with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
        zf.extractall(extract_dir)
    return [p for p in extract_dir.rglob("*") if p.is_file() and not p.name.startswith(".")]


def load_hash_cache(service, folder_id: str) -> tuple[set[str], str | None]:
    """
    Load hashes.json from the output folder if it exists.
    Returns (set_of_hashes, file_id_of_cache_or_None).
    """
    files = list_files(service, folder_id)
    cache_files = [f for f in files if f["name"] == HASH_CACHE_NAME]
    if cache_files:
        file_id = cache_files[0]["id"]
        print(f"Found hash cache ({HASH_CACHE_NAME}), loading...", end=" ", flush=True)
        data = download_file(service, file_id)
        hashes = set(json.loads(data.decode()))
        print(f"{len(hashes)} hashes loaded.")
        return hashes, file_id
    return set(), None


def save_hash_cache(service, hashes: set[str], folder_id: str, cache_file_id: str | None) -> str:
    """Save hashes.json to the output folder, updating in place if it already exists.
    Returns the file ID of the cache file."""
    data = json.dumps(sorted(hashes), indent=2).encode()
    if cache_file_id:
        update_file(service, cache_file_id, data, "application/json")
        return cache_file_id
    else:
        file_id = upload_file(service, HASH_CACHE_NAME, data, "application/json", folder_id)
        return file_id


def build_hash_set_from_folder(service, folder_id: str) -> set[str]:
    """Fallback: hash all files in the output folder (used when no cache exists)."""
    files = list_files(service, folder_id)
    files = [f for f in files if not f["mimeType"].startswith("application/vnd.google-apps")
             and f["name"] != HASH_CACHE_NAME]
    print(f"No cache found. Hashing {len(files)} existing files...")
    hashes = set()
    for f in files:
        print(f"  Hashing  {f['name']}...", end=" ", flush=True)
        data = download_file(service, f["id"])
        hashes.add(sha256_bytes(data))
        print("done.")
    print(f"Files indexed: {len(hashes)}\n")
    return hashes


def run(zip_folder_id: str, output_folder_id: str) -> None:
    service = get_service()

    # Step 1: Find all zips in the input folder
    zips = get_zips_from_folder(service, zip_folder_id)
    print()

    # Step 2: Load hash cache once (or build from scratch)
    existing_hashes, cache_file_id = load_hash_cache(service, output_folder_id)
    if not existing_hashes:
        existing_hashes = build_hash_set_from_folder(service, output_folder_id)

    total_uploaded = 0
    total_skipped = 0

    # Step 3: Process each zip one by one
    for i, zip_entry in enumerate(zips, 1):
        print(f"── Zip {i}/{len(zips)}: {zip_entry['name']} ──")
        print("Downloading...", end=" ", flush=True)
        zip_data = download_file(service, zip_entry["id"])
        print("done.")

        with tempfile.TemporaryDirectory() as tmpdir:
            extract_dir = Path(tmpdir)
            print("Extracting...", end=" ", flush=True)
            new_files = extract_zip(zip_data, extract_dir)
            print(f"done. {len(new_files)} files extracted.")

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
                    cache_file_id = save_hash_cache(service, existing_hashes, output_folder_id, cache_file_id)
                    print("done.")
                    uploaded += 1

        print(f"  → Uploaded: {uploaded}  Skipped: {skipped}\n")
        total_uploaded += uploaded
        total_skipped += skipped

    print(f"── All done ──")
    print(f"Total uploaded : {total_uploaded}")
    print(f"Total skipped  : {total_skipped}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 dedupe_gdrive.py <zip_folder_id> <output_folder_id>")
        print()
        print("  zip_folder_id    — Drive folder containing the zip of new files")
        print("  output_folder_id — Drive folder to compare against and upload new unique files into")
        sys.exit(1)

    run(sys.argv[1], sys.argv[2])
