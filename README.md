# gdrive-dedupe

Deduplicate files in a Google Drive folder by content hash (SHA-256).
Unique files are copied to a destination folder, renamed to their hash.

## Quick start

```bash
git clone <repo-url>
cd gdrive-dedupe
chmod +x setup.sh && ./setup.sh
```

Follow the instructions printed by setup to add your `credentials.json`, then:

```bash
source .venv/bin/activate
python3 dedupe_gdrive.py <FOLDER_ID>
```

The folder ID is the last segment of your Drive URL:
```
https://drive.google.com/drive/folders/<FOLDER_ID>
```

## Usage

```
python3 dedupe_gdrive.py <source_folder_id> [dest_folder_name]
```

| Argument | Default | Description |
|---|---|---|
| `source_folder_id` | *(required)* | Drive folder to deduplicate |
| `dest_folder_name` | `y` | Name of destination folder (created next to source) |

## What it does

1. Lists all files in the source folder
2. Downloads each file and computes its SHA-256 hash
3. Skips duplicates (same content = same hash)
4. Uploads unique files to the destination folder, named `<hash>.<ext>`

Google Docs / Sheets / Slides are skipped (they cannot be downloaded as raw bytes).

## First-run authentication

On first run a browser window will open for Google OAuth consent.
Your token is saved to `token.json` so subsequent runs are silent.

**Never commit `credentials.json` or `token.json`** — both are in `.gitignore`.

## Requirements

- Python 3.10+
- A Google Cloud project with the Drive API enabled
- OAuth 2.0 Desktop credentials (`credentials.json`)
