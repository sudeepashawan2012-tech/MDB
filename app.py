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
        
        # Standardize columns immediately to match your old code logic
        df.columns = [c.strip().replace(' ', '_').replace('.', '') for c in df.columns] 
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
        # 1. HOME
        if report_choice == "🏠 Home":
            st.subheader("🔍 Search Inventory")
            search = st.text_input("Search Style/Bag...", placeholder="Type here...")
            display_df = df.copy()
            if search:
                display_df = display_df[display_df['Style_No'].astype(str).str.contains(search, case=False) | 
                                        display_df['Bag_No'].astype(str).str.contains(search, case=False)]
            st.dataframe(display_df, use_container_width=True, hide_index=True)

        # 2. BAG HISTORY
        elif report_choice == "🔍 Bag History Report":
            st.subheader("🔍 Bag History Report")
            search_bag = st.text_input("Search Bag Number").strip()
            if search_bag:
                m_data = df[df['Bag_No'].astype(str) == search_bag]
                if not m_data.empty:
                    row = m_data.iloc[0]
                    st.markdown('<div class="section-head">Bag Details</div>', unsafe_allow_html=True)
                    c1, c2 = st.columns([2, 1])
                    c1.markdown(f"### Bag NO: `{search_bag}`")
                    c2.warning(f"**Status:** {row.get('Final_VZ_Status', 'N/A')}")
                    
                    colA, colB = st.columns(2)
                    with colA:
                        st.write(f"1. **Customer:** {get_val(row.get('Customer'))}", unsafe_allow_html=True)
                        st.write(f"2. **Type:** {get_val(row.get('Order_Type'))}", unsafe_allow_html=True)
                    with colB:
                        st.write(f"3. **Metal:** {get_val(row.get('Metal'))}g", unsafe_allow_html=True)
                        st.write(f"4. **Dia Cts:** {get_val(row.get('Dia_Cts'))}", unsafe_allow_html=True)
                else: st.error("Bag not found.")

        # 3. METAL REQUIREMENTS (Fixed with 18kt/Pure calculation)
        elif report_choice == "📊 Metal Requirements":
            st.subheader("📊 Metal Requirements")
            df['Metal'] = pd.to_numeric(df['Metal'], errors='coerce').fillna(0)
            pending_df = df[(df['Metal_Issue_Date'].isna()) | (df['Final_VZ_Status'] == "METAL PENDING")].copy()

            def create_metal_card(data, label):
                summary = data.groupby('Customer').agg({'Bag_No': 'count', 'Metal': 'sum'})
                summary['Metal 18kt'] = summary['Metal'].apply(std_round)
                summary['Pure'] = (summary['Metal 18kt'] * 0.76).apply(std_round)
                c1, c2 = st.columns(2)
                c1.metric(f"{label} Bags", summary['Bag_No'].sum())
                c2.metric(f"18kt Total", f"{summary['Metal 18kt'].sum()}g")
                st.table(summary[['Bag_No', 'Metal 18kt', 'Pure']].rename(columns={'Bag_No': 'Qty'}))

            st.info("👤 CUSTOMER ORDERS")
            c_df = pending_df[pending_df['Order_Type'].str.contains('CUSTOMER', case=False, na=False)]
            if not c_df.empty: create_metal_card(c_df, "Cust")
            
            st.warning("📦 STOCK ORDERS")
            s_df = pending_df[pending_df['Order_Type'].str.contains('STOCK', case=False, na=False)]
            if not s_df.empty: create_metal_card(s_df, "Stock")

        # 4. CSR (Fixed Sorting and Column Error)
        elif report_choice == "📋 CSR":
            st.subheader("📋 Customer Status Report")
            status_seq = {"SEQUENCE": 0, "ENGRAVING/HUID": 1, "IGI": 2, "ON HAND": 3, "FINAL QC": 4, "SETTING QC OK": 5, "SETTING": 6, "GHAT OK": 7, "CASTING": 8, "METAL ISSUED": 9, "METAL PENDING": 10}
            csr_df = df.copy()
            csr_df['Metal'] = pd.to_numeric(csr_df['Metal'], errors='coerce').fillna(0)
            csr_df['Dia_Cts'] = pd.to_numeric(csr_df['Dia_Cts'], errors='coerce').fillna(0)
            
            # Use .get() to avoid KeyError if column is missing
            status_col = 'Final_VZ_Status'
            csr_df['Seq'] = csr_df[status_col].map(status_seq).fillna(99)

            for cust in sorted(csr_df['Customer'].unique()):
                with st.expander(f"👤 {cust}"):
                    cust_data = csr_df[csr_df['Customer'] == cust]
                    summary = cust_data.groupby([status_col, 'Seq']).agg({'Bag_No': 'count', 'Metal': 'sum', 'Dia_Cts': 'sum'}).reset_index().sort_values('Seq')
                    summary['Metal 18kt'] = summary['Metal'].apply(std_round)
                    summary['Dia Cts'] = summary['Dia_Cts'].map('{:,.2f}'.format)
                    st.table(summary[[status_col, 'Bag_No', 'Metal 18kt', 'Dia Cts']].rename(columns={status_col: 'Status', 'Bag_No': 'Qty'}))
                    st.markdown(f"**TOTAL:** `{summary['Bag_No'].sum()}` Bags | `{summary['Metal 18kt'].sum()}g` 18kt")
