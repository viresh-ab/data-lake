import os
import json
import logging
from typing import Optional, List, Dict
import requests
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import msal

# ------------------------
# FastAPI app setup
# ------------------------
app = FastAPI(title="SharePoint / Data Lake API")
templates = Jinja2Templates(directory="templates")

@app.get("/privacy-policy", response_class=HTMLResponse)
async def privacy_page(request: Request):
    return templates.TemplateResponse("privacy.html", {"request": request})

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
# Logging & CORS setup
# ------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("data-lake-api")

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
    """Acquire a token using client credentials flow."""
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
    """Recursively fetch all drive items starting from folder_id."""
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
            # Recurse into folder
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
    Use Graph path addressing: /drives/{driveId}/root:/{path}:
    Automatically prepends 'Documents/' if not present (for SharePoint).
    """
    safe_path = path.lstrip("/")
    if not safe_path.startswith("Documents/"):
        safe_path = f"Documents/{safe_path}"

    url = f"{GRAPH_BASE}/drives/{drive_id}/root:/{safe_path}:"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    resp = requests.get(url, headers=headers, timeout=30)

    if resp.status_code >= 400:
        logger.error("Graph GET error %s: %s", resp.status_code, resp.text)
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return resp.json()

# ------------------------
# API Endpoints
# ------------------------
@app.get("/", summary="Health check")
def root():
    return {"status": "ok", "message": "Data Lake API running"}

@app.get("/list-files", summary="List files recursively in folder (or root)")
def list_files(folder: Optional[str] = Query(None, description="Folder path under root; e.g. CASE_STUDIES or nested/path")):
    token = get_access_token()
    if not folder:
        items = fetch_all_items(token, DRIVE_ID, folder_id="root", path="")
        return {"count": len(items), "items": items}

    try:
        item = get_item_by_path(token, DRIVE_ID, folder)
    except HTTPException as e:
        raise HTTPException(status_code=404, detail=f"Folder '{folder}' not found: {e.detail}")

    if "file" in item and "folder" not in item:
        raise HTTPException(status_code=400, detail=f"'{folder}' is a file, not a folder")

    folder_id = item["id"]
    items = fetch_all_items(token, DRIVE_ID, folder_id=f"items/{folder_id}", path=folder)
    return {"count": len(items), "items": items}


@app.get("/metadata", summary="Load metadata from SharePoint or local fallback")
def metadata(file_path: Optional[str] = Query(None, description="metadata.json path")):
    token = get_access_token()
    try:
        if file_path:
            # Try to fetch from SharePoint
            item = get_item_by_path(token, DRIVE_ID, file_path)
            download_url = item.get("@microsoft.graph.downloadUrl")
            if download_url:
                resp = requests.get(download_url, timeout=30)
                resp.raise_for_status()
                return {"file_path": file_path, "metadata": json.loads(resp.text)}

        # ✅ Local fallback
        if os.path.exists("metadata.json"):
            with open("metadata.json", "r", encoding="utf-8") as f:
                data = json.load(f)
            return {"file_path": "local/metadata.json", "metadata": data}

        raise HTTPException(status_code=404, detail="metadata.json not found locally or in SharePoint")

    except Exception as e:
        logger.exception("Error loading metadata")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/download", summary="Return a file's direct download URL")
def download(file_path: str = Query(..., description="Full path under root, e.g. CASE_STUDIES/abc.pdf")):
    token = get_access_token()
    try:
        item = get_item_by_path(token, DRIVE_ID, file_path)
        download_url = item.get("@microsoft.graph.downloadUrl")

        if not download_url:
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
        logger.exception("Download error")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/search", summary="Search items by query")
def search(q: str = Query(..., description="Search term (file/folder name)")):
    token = get_access_token()
    url = f"{GRAPH_BASE}/drives/{DRIVE_ID}/root/search(q='{q}')"
    data = graph_get(url, token)
    hits = data.get("value", [])

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

