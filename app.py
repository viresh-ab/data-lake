import os
import json
import logging
from typing import Optional, List, Dict

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import msal

# ------------------------
# Config (from env vars)
# ------------------------
TENANT_ID = os.getenv("TENANT_ID", "7733019f-9439-44b2-adf1-5f80b662cf10")
CLIENT_ID = os.getenv("CLIENT_ID", "e394941b-ed05-408e-a17e-5bbe2d7ee0c2")
CLIENT_SECRET = os.getenv("CLIENT_SECRET", "lDC8Q~t_sMD1D3As7_J9p663Z_4oy1rODCGjDchi")
DRIVE_ID = os.getenv("DRIVE_ID", "b!bubewMnBgU-zI6wK9kbV3Ghs1L66d-BAoHMsQBC960t_eqKN0h8GS6obytpMgNDQ")

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPES = ["https://graph.microsoft.com/.default"]
GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# ------------------------
# Logging & FastAPI init
# ------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("data-lake-api")

app = FastAPI(title="SharePoint / Data Lake API")

# CORS: allow all for now (public API). Change in production if needed.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

# ------------------------
# MSAL / Token helper
# ------------------------
def get_access_token() -> str:
    """
    Acquire a token for client credentials flow.
    """
    app_msal = msal.ConfidentialClientApplication(
        client_id=CLIENT_ID,
        authority=AUTHORITY,
        client_credential=CLIENT_SECRET
    )
    result = app_msal.acquire_token_for_client(scopes=SCOPES)
    if "access_token" not in result:
        logger.error("MSAL token error: %s", result)
        raise HTTPException(status_code=500, detail="Failed to acquire access token")
    return result["access_token"]

def graph_get(url: str, token: str, params: dict = None) -> dict:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    resp = requests.get(url, headers=headers, params=params, timeout=30)
    if resp.status_code >= 400:
        logger.error("Graph GET error %s: %s", resp.status_code, resp.text)
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return resp.json()

# ------------------------
# Graph helpers
# ------------------------
def fetch_all_items(token: str, drive_id: str, folder_id: str = "root", path: str = "") -> List[Dict]:
    """
    Recursively fetch drive items starting from folder_id.
    folder_id can be 'root' or 'items/{id}'.
    """
    all_items = []
    url = f"{GRAPH_BASE}/drives/{drive_id}/{folder_id}/children"
    data = graph_get(url, token)

    for item in data.get("value", []):
        current_path = f"{path}/{item['name']}".lstrip("/")
        if "folder" in item:
            all_items.append({
                "type": "folder",
                "name": item["name"],
                "path": current_path,
                "id": item["id"]
            })
            # recurse into folder
            all_items.extend(fetch_all_items(token, drive_id, f"items/{item['id']}", current_path))
        else:
            all_items.append({
                "type": "file",
                "name": item["name"],
                "path": current_path,
                "mimeType": item.get("file", {}).get("mimeType"),
                "size_mb": round(item.get("size", 0) / (1024 * 1024), 2),
                "downloadUrl": item.get("@microsoft.graph.downloadUrl"),
                "id": item["id"]
            })
    return all_items

def get_item_by_path(token: str, drive_id: str, path: str) -> dict:
    """
    Use Graph path addressing: /drives/{driveId}/root:/{path}
    path should NOT start with a leading slash.
    """
    safe_path = path.lstrip("/")
    url = f"{GRAPH_BASE}/drives/{drive_id}/root:/{safe_path}"
    # Add /? to avoid redirect differences — Graph will return item
    return graph_get(url, token)

# ------------------------
# API endpoints
# ------------------------
@app.get("/", summary="Health check")
def root():
    return {"status": "ok", "message": "Data Lake API running"}

@app.get("/list-files", summary="List files recursively in folder (or root)")
def list_files(folder: Optional[str] = Query(None, description="Folder path under root; e.g. CASE_STUDIES or nested/path")):
    """
    If folder is omitted, returns entire drive recursively.
    If folder provided, returns recursively starting from that path.
    """
    token = get_access_token()
    if not folder:
        items = fetch_all_items(token, DRIVE_ID, folder_id="root", path="")
        return {"count": len(items), "items": items}

    # Try to resolve the folder path to an item; it may be a folder path
    try:
        item = get_item_by_path(token, DRIVE_ID, folder)
    except HTTPException as e:
        raise HTTPException(status_code=404, detail=f"Folder '{folder}' not found: {e.detail}")

    # If it's a file, return error
    if "file" in item and "folder" not in item:
        raise HTTPException(status_code=400, detail=f"'{folder}' is a file, not a folder")

    # Start recursion from this folder's id
    folder_id = item["id"]
    items = fetch_all_items(token, DRIVE_ID, folder_id=f"items/{folder_id}", path=folder)
    return {"count": len(items), "items": items}

@app.get("/metadata", summary="Load metadata.json for a file path or return metadata.json content from drive")
def metadata(file_path: Optional[str] = Query(None, description="If file_path points to a metadata.json file, returns it. If omitted, tries to find metadata.json in root via search")):
    token = get_access_token()
    try:
        if file_path:
            # fetch the file item by path and return its content if it's JSON
            item = get_item_by_path(token, DRIVE_ID, file_path)
            # get downloadUrl from item
            download_url = item.get("@microsoft.graph.downloadUrl")
            if not download_url:
                raise HTTPException(status_code=404, detail="Download URL not available for the requested file.")
            # download content
            resp = requests.get(download_url, timeout=30)
            resp.raise_for_status()
            content = resp.content.decode("utf-8")
            try:
                parsed = json.loads(content)
                return {"file_path": file_path, "metadata": parsed}
            except json.JSONDecodeError:
                raise HTTPException(status_code=400, detail="Requested file is not valid JSON")
        else:
            # no path given: search for metadata.json at drive root (or anywhere) and return the first match
            # Graph search requires encoded q param
            url = f"{GRAPH_BASE}/drives/{DRIVE_ID}/root/search(q='metadata.json')"
            resp = graph_get(url, token)
            hits = resp.get("value", [])
            if not hits:
                raise HTTPException(status_code=404, detail="metadata.json not found in drive")
            file_id = hits[0]["id"]
            # fetch item to get downloadUrl
            item = graph_get(f"{GRAPH_BASE}/drives/{DRIVE_ID}/items/{file_id}", token)
            download_url = item.get("@microsoft.graph.downloadUrl")
            if not download_url:
                raise HTTPException(status_code=404, detail="Download URL missing for metadata.json")
            r = requests.get(download_url, timeout=30)
            r.raise_for_status()
            parsed = json.loads(r.content.decode("utf-8"))
            return {"file_path": hits[0].get("parentReference", {}).get("path", ""), "metadata": parsed}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error loading metadata")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/download", summary="Return a file's direct download URL")
def download(file_path: str = Query(..., description="Full path under root, e.g. CASE_STUDIES/abc.pdf")):
    """
    Returns JSON with a direct download URL for the file. The GPT/consumer can use that URL to fetch the file content.
    """
    token = get_access_token()
    try:
        item = get_item_by_path(token, DRIVE_ID, file_path)
        # Try to get the download URL
        # Some Graph responses include @microsoft.graph.downloadUrl directly, others require an extra call.
        download_url = item.get("@microsoft.graph.downloadUrl")
        if not download_url:
            # fetch item by id
            item_id = item.get("id")
            if not item_id:
                raise HTTPException(status_code=404, detail="Item not found")
            item2 = graph_get(f"{GRAPH_BASE}/drives/{DRIVE_ID}/items/{item_id}", token)
            download_url = item2.get("@microsoft.graph.downloadUrl")
            if not download_url:
                raise HTTPException(status_code=404, detail="Download URL not available")
        return {"file_path": file_path, "download_url": download_url}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("download error")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/search", summary="Search items by query")
def search(q: str = Query(..., description="Search term (file/folder name)")):
    token = get_access_token()
    url = f"{GRAPH_BASE}/drives/{DRIVE_ID}/root/search(q='{q}')"
    data = graph_get(url, token)
    hits = data.get("value", [])
    # Return a lightweight representation
    results = []
    for item in hits:
        parent_ref = item.get("parentReference", {})
        results.append({
            "id": item.get("id"),
            "name": item.get("name"),
            "path": parent_ref.get("path"),
            "type": "folder" if item.get("folder") else "file",
            "downloadUrl": item.get("@microsoft.graph.downloadUrl")
        })
    return {"count": len(results), "results": results}
