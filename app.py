import streamlit as st
import msal
import requests
import json

# =========================
# AZURE AD CONFIG
# =========================
TENANT_ID = "7733019f-9439-44b2-adf1-5f80b662cf10"
CLIENT_ID = "e394941b-ed05-408e-a17e-5bbe2d7ee0c2"
CLIENT_SECRET = "lDC8Q~t_sMD1D3As7_J9p663Z_4oy1rODCGjDchi"

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPES = ["https://graph.microsoft.com/.default"]

DRIVE_ID = "b!bubewMnBgU-zI6wK9kbV3Ghs1L66d-BAoHMsQBC960t_eqKN0h8GS6obytpMgNDQ"


# =========================
# AUTH TOKEN
# =========================
@st.cache_data(show_spinner=False)
def get_access_token():
    app = msal.ConfidentialClientApplication(
        client_id=CLIENT_ID,
        authority=AUTHORITY,
        client_credential=CLIENT_SECRET
    )
    result = app.acquire_token_for_client(scopes=SCOPES)
    if "access_token" not in result:
        raise Exception(result)
    return result["access_token"]


# =========================
# GRAPH GET helper
# =========================
def graph_get(url, token):
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        raise Exception(r.text)
    return r.json()


# =========================
# RECURSIVE DRIVE FETCH
# =========================
def fetch_all_items(token, drive_id, folder_id="root", path=""):
    all_items = []
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/{folder_id}/children"
    data = graph_get(url, token)

    for item in data.get("value", []):
        current_path = f"{path}/{item['name']}"

        if "folder" in item:
            all_items.append({
                "type": "folder",
                "name": item["name"],
                "path": current_path
            })

            all_items.extend(
                fetch_all_items(token, drive_id, f"items/{item['id']}", current_path)
            )

        else:
            all_items.append({
                "type": "file",
                "name": item["name"],
                "path": current_path,
                "extension": item.get("file", {}).get("mimeType"),
                "size_mb": round(item.get("size", 0) / (1024 * 1024), 2),
                "downloadUrl": item.get("@microsoft.graph.downloadUrl"),
                "id": item["id"]
            })

    return all_items


# =========================
# SEARCH FILE BY NAME
# =========================
def search_file_in_drive(token, drive_id, filename):
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root/search(q='{filename}')"
    data = graph_get(url, token)
    return data.get("value", [])


# =========================
# DOWNLOAD FILE
# =========================
def download_file(token, drive_id, file_id):
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{file_id}"
    data = graph_get(url, token)
    download_url = data.get("@microsoft.graph.downloadUrl")
    if not download_url:
        raise Exception("Download URL missing")
    return requests.get(download_url).content


# =========================
# STREAMLIT UI
# =========================
st.set_page_config(page_title="SharePoint Auto Loader", layout="wide")
st.title("🗂 SharePoint Drive – Auto Data Loader")


# =====================================================
# AUTO LOAD: DRIVE DATA (NO BUTTON)
# =====================================================
st.subheader("📂 Auto-loaded Drive Inventory")

try:
    token = get_access_token()

    with st.spinner("Fetching full drive data..."):
        all_items = fetch_all_items(token, DRIVE_ID)

    st.success(f"Total items found in drive: {len(all_items)}")

    for item in all_items:
        if item["type"] == "folder":
            st.write(f"📁 **{item['path']}**")
        else:
            st.write(f"📄 {item['path']}  — {item['size_mb']} MB")

    with st.expander("📦 Raw Drive JSON"):
        st.json(all_items)

except Exception as e:
    st.error(f"❌ Error loading drive data: {e}")


# =====================================================
# AUTO LOAD: metadata.json (NO BUTTON)
# =====================================================
st.subheader("📥 Auto-loaded JSON: metadata.json")

AUTO_JSON_NAME = "metadata.json"

try:
    st.info(f"Searching for `{AUTO_JSON_NAME}`...")

    results = search_file_in_drive(token, DRIVE_ID, AUTO_JSON_NAME)

    if not results:
        st.error(f"❌ {AUTO_JSON_NAME} not found!")
    else:
        file_id = results[0]["id"]

        with st.spinner("Downloading metadata.json..."):
            content = download_file(token, DRIVE_ID, file_id)

        json_data = json.loads(content.decode("utf-8"))

        st.success("Loaded metadata.json successfully!")
        st.json(json_data)

except Exception as e:
    st.error(f"❌ Error auto-loading JSON: {e}")
