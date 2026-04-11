import streamlit as st
import pandas as pd
import numpy as np
import requests
from google.cloud import bigquery
from google.oauth2 import service_account

# 1. SETUP & PAGE CONFIG
st.set_page_config(page_title="MASTER DATABASE", layout="wide", initial_sidebar_state="collapsed")

# --- SQL FETCH FUNCTION ---
@st.cache_data(ttl=300)
def fetch_master_from_sql():
    try:
        scopes = [
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/spreadsheets",
        ]
        creds = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"], 
            scopes=scopes
        )
        client = bigquery.Client(credentials=creds, project=creds.project_id)
        query = "SELECT * FROM `jewelry-sql-system.workshop_data.master_inventory`"
        df = client.query(query).to_dataframe()
        
        # CLEANING: Force columns to be uppercase and replace spaces with underscores
        df.columns = [c.strip().upper().replace(' ', '_') for c in df.columns] 
        return df
    except Exception as e:
        st.error(f"SQL Error: {e}")
        return None

# --- HISTORY FETCH FUNCTION ---
@st.cache_data(ttl=300)
def fetch_history(urls_tuple):
    try:
        frames = [pd.read_csv(url, skiprows=1, header=None) for url in urls_tuple]
        return pd.concat(frames, ignore_index=True)
    except:
        return None

# --- HELPER FUNCTIONS ---
def get_val(val):
    if pd.isna(val) or str(val).strip().lower() in ['nan', '', 'none', 'pending']:
        return '<span class="missing-data">X</span>'
    return str(val)

def std_round(x):
    try: return int(float(x) + 0.5) if float(x) > 0 else 0
    except: return 0

# --- AUTHENTICATION ---
if "password_correct" not in st.session_state:
    st.title("🔒 Master Structure & Reports")
    pwd = st.text_input("Workshop Password", type="password")
    if st.button("Login"):
        if pwd == st.secrets["workshop_password"]: 
            st.session_state["password_correct"] = True
            st.rerun()
        else: st.error("Invalid Password")
else:
    # 2. RUN APP
    df = fetch_master_from_sql()
    
    # Static URLs
    LIVE_URLS = (
        "https://docs.google.com/spreadsheets/d/e/2PACX-1vRk0Sr33sugG2FtNtdqqk11u8b2aGKGniTN1n2qcWiOCg1W0Vi5JzqLWZdu1DWVUfpP2baURyOn4qo7/pub?gid=449683950&single=true&output=csv",
        "https://docs.google.com/spreadsheets/d/e/2PACX-1vRk0Sr33sugG2FtNtdqqk11u8b2aGKGniTN1n2qcWiOCg1W0Vi5JzqLWZdu1DWVUfpP2baURyOn4qo7/pub?gid=1700828894&single=true&output=csv"
    )
    POST_URLS = (
        "https://docs.google.com/spreadsheets/d/e/2PACX-1vRk0Sr33sugG2FtNtdqqk11u8b2aGKGniTN1n2qcWiOCg1W0Vi5JzqLWZdu1DWVUfpP2baURyOn4qo7/pub?gid=1461249957&single=true&output=csv",
        "https://docs.google.com/spreadsheets/d/e/2PACX-1vRk0Sr33sugG2FtNtdqqk11u8b2aGKGniTN1n2qcWiOCg1W0Vi5JzqLWZdu1DWVUfpP2baURyOn4qo7/pub?gid=163646832&single=true&output=csv"
    )
    
    df_pre = fetch_history(LIVE_URLS)
    df_post = fetch_history(POST_URLS)

    # SIDEBAR
    st.sidebar.title("💼 ADMIN")
    report_choice = st.sidebar.selectbox("GO TO:", ["🏠 Home", "🔍 Bag History Report", "📊 Metal Requirements", "📋 CSR"])
    
    if st.sidebar.button("Logout"):
        del st.session_state["password_correct"]
        st.rerun()

    # CUSTOM CSS
    st.markdown("""<style>
        [data-testid="stMetric"] { background-color: rgba(125, 125, 125, 0.1); padding: 10px; border-radius: 8px; }
        .section-head { background-color: #2e2e2e; color: #f0f0f0; padding: 8px 15px; border-radius: 5px; margin: 20px 0 10px 0; font-weight: bold; }
        .missing-data { color: #ff4b4b; font-weight: bold; }
        </style>""", unsafe_allow_html=True)

    if df is not None:
        # MAP COLUMNS (Finds columns even if names vary slightly)
        def find_col(possible_names):
            for name in possible_names:
                for col in df.columns:
                    if name.upper() in col: return col
            return df.columns[0]

        col_bag = find_col(['BAG_NO', 'BAG'])
        col_style = find_col(['STYLE_NO', 'STYLE'])
        col_status = find_col(['FINAL_VZ_STATUS', 'STATUS'])
        col_metal_date = find_col(['METAL_ISSUE_DATE', 'ISSUE_DATE'])
        col_metal = find_col(['METAL', 'WEIGHT'])
        col_customer = find_col(['CUSTOMER'])
        col_type = find_col(['ORDER_TYPE', 'TYPE'])
        col_dia = find_col(['DIA_CTS', 'DIA'])

        if report_choice == "🏠 Home":
            st.subheader("🔍 Search Inventory")
            search = st.text_input("Search Style/Bag...", placeholder="Type here...")
            display_df = df.copy()
            if search:
                display_df = display_df[display_df[col_style].astype(str).str.contains(search, case=False) | 
                                        display_df[col_bag].astype(str).str.contains(search, case=False)]
            st.dataframe(display_df, use_container_width=True, hide_index=True)

        elif report_choice == "🔍 Bag History Report":
            st.subheader("🔍 Bag History Report")
            search_bag = st.text_input("Enter Bag Number").strip()
            if search_bag:
                m_data = df[df[col_bag].astype(str) == search_bag]
                if not m_data.empty:
                    row = m_data.iloc[0]
                    st.markdown('<div class="section-head">Bag Details</div>', unsafe_allow_html=True)
                    st.write(f"**Bag NO:** `{search_bag}` | **Status:** `{row[col_status]}`")
                    # Movement logic here...
                else: st.error("Bag not found.")

        elif report_choice == "📊 Metal Requirements":
            st.subheader("📊 Metal Requirements")
            # Using the dynamic column names
            df[col_metal] = pd.to_numeric(df[col_metal], errors='coerce').fillna(0)
            p_df = df[(df[col_metal_date].isna()) | (df[col_status].astype(str).str.contains("PENDING", na=False))].copy()
            
            if not p_df.empty:
                summary = p_df.groupby(col_customer).agg({col_bag: 'count', col_metal: 'sum'})
                st.table(summary)
            else: st.success("No metal pending!")

        elif report_choice == "📋 CSR":
            st.subheader("📋 Customer Status Report")
            status_seq = {"SEQUENCE": 0, "ENGRAVING/HUID": 1, "IGI": 2, "ON HAND": 3, "FINAL QC": 4, "SETTING QC OK": 5, "SETTING": 6, "GHAT OK": 7, "CASTING": 8, "METAL ISSUED": 9, "METAL PENDING": 10}
            
            csr_df = df.copy()
            csr_df[col_metal] = pd.to_numeric(csr_df[col_metal], errors='coerce').fillna(0)
            # Use the dynamic status column
            csr_df['Seq'] = csr_df[col_status].astype(str).str.upper().map(status_seq).fillna(99)
            
            for cust in sorted(csr_df[col_customer].unique()):
                with st.expander(f"👤 {cust}"):
                    cust_data = csr_df[csr_df[col_customer] == cust]
                    summary = cust_data.groupby([col_status, 'Seq']).agg({col_bag: 'count', col_metal: 'sum'}).reset_index().sort_values('Seq')
                    st.table(summary.drop(columns=['Seq']))
