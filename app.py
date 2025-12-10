import streamlit as st
import msal
import requests
import json

# =========================
# AZURE AD CONFIG
# =========================
TENANT_ID = st.secrets["TENANT_ID"]
CLIENT_ID = st.secrets["CLIENT_ID"]
CLIENT_SECRET = st.secrets["CLIENT_SECRET"]

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPES = ["https://graph.microsoft.com/.default"]

SITE_PATH = (
    "sites/markelytics.sharepoint.com,"
    "ffae9f78-1119-4335-9fab-b3c83cb0bbf3,"
    "bed46c68-77ba-40e0-a073-2c4010bdebdffss"
)

# =========================
# FUNCTIONS
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

@st.cache_data(show_spinner=False)
def get_site_drive(token):
    url = f"https://graph.microsoft.com/v1.0/{SITE_PATH}/drive"

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }

    r = requests.get(url, headers=headers)

    if r.status_code != 200:
        raise Exception(r.text)

    return r.json()

@st.cache_data(show_spinner=False)
def list_root_files(token, drive_id):
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children"

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }

    r = requests.get(url, headers=headers)

    if r.status_code != 200:
        raise Exception(r.text)

    return r.json()

# =========================
# STREAMLIT UI
# =========================
st.set_page_config(page_title="SharePoint Drive Explorer", layout="wide")

st.title("📁 SharePoint Drive Explorer")

st.markdown(
    """
    This app connects to **Microsoft Graph** using **Azure AD (Client Credentials)**  
    and lists files from a SharePoint site.
    """
)

if st.button("🔐 Connect & Fetch Files"):
    try:
        with st.spinner("Getting access token..."):
            token = get_access_token()

        st.success("Access token acquired ✅")

        with st.spinner("Fetching site drive..."):
            drive = get_site_drive(token)

        drive_id = drive["id"]
        st.success(f"Drive loaded: **{drive['name']}**")

        with st.spinner("Listing files..."):
            files = list_root_files(token, drive_id)

        st.subheader("📄 Files & Folders")
        for item in files.get("value", []):
            if "folder" in item:
                st.write(f"📁 **{item['name']}**")
            else:
                st.write(f"📄 {item['name']}")

        # Show raw JSON (optional)
        with st.expander("🔎 Raw JSON response"):
            st.json(files)

        # Download button
        st.download_button(
            label="⬇️ Download JSON",
            data=json.dumps(files, indent=2),
            file_name="sharepoint_files.json",
            mime="application/json"
        )

    except Exception as e:
        st.error(f"❌ Error: {e}")

