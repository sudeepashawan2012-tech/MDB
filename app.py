import streamlit as st
import pandas as pd
import numpy as np
import requests
from google.cloud import bigquery
from google.oauth2 import service_account

# 1. SETUP & PAGE CONFIG
st.set_page_config(page_title="JDS MASTER DATABASE", layout="wide", initial_sidebar_state="collapsed")

# --- SQL FETCH FUNCTION ---
@st.cache_data(ttl=300) # Increased cache to 5 mins for speed
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
        
        # Clean column names (Remove spaces and dots)
        df.columns = [c.strip().replace(' ', '_').replace('.', '') for c in df.columns] 
        return df
    except Exception as e:
        st.error(f"SQL Error: {e}")
        return None

# --- HISTORY FETCH FUNCTION ---
@st.cache_data(ttl=300)
def fetch_history(urls_tuple): # Use tuple for better caching
    try:
        frames = []
        for url in urls_tuple:
            tdf = pd.read_csv(url, skiprows=1, header=None)
            frames.append(tdf)
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

def show_history_tables(hist_df, label, search_bag, mode):
    st.markdown(f'<div class="section-head">{label} MOVEMENT</div>', unsafe_allow_html=True)
    if hist_df is not None:
        if mode == "PRE": bag_col, in_p, in_d, in_t, out_p, out_d, out_t = 2, 8, 12, 13, 18, 21, 22
        else: bag_col, in_p, in_d, in_t, out_p, out_d, out_t = 1, 17, 20, 21, 7, 11, 12
        search_bag_str = str(search_bag).strip()
        moves = hist_df[hist_df[bag_col].astype(str).str.strip() == search_bag_str].copy()
        if not moves.empty:
            h_in, h_out = st.columns(2)
            with h_in:
                st.info("Inward")
                st.table(pd.DataFrame({'Date': moves[in_d], 'Time': moves[in_t], 'Purpose': moves[in_p]}).dropna(subset=['Date']).fillna("-"))
            with h_out:
                st.error("Outward")
                st.table(pd.DataFrame({'Date': moves[out_d], 'Time': moves[out_t], 'Purpose': moves[out_p]}).dropna(subset=['Date']).fillna("-"))
        else: st.info(f"No {label} logs found.")

# --- AUTHENTICATION ---
if "password_correct" not in st.session_state:
    st.title("🔒 Master Structure & Reports")
    pwd = st.text_input("Workshop Password", type="password")
    if st.button("Login"):
        if pwd == st.secrets["workshop_password"]: 
            st.session_state["password_correct"] = True
            st.rerun()
        else:
            st.error("Invalid Password")
else:
    # 2. RUN APP ONLY AFTER LOGIN
    # --- DATA LOADING ---
    df = fetch_master_from_sql()
    
    # Static URLs for history
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
        # Check for column names to avoid KeyError
        metal_col = 'Metal' if 'Metal' in df.columns else df.columns[df.columns.str.contains('Metal', case=False)][0]
        dia_col = 'Dia_Cts' if 'Dia_Cts' in df.columns else df.columns[df.columns.str.contains('Dia', case=False)][0]

        if report_choice == "🏠 Home":
            st.subheader("🔍 Search Inventory")
            search = st.text_input("Search Style/Bag...", placeholder="Type here...")
            display_df = df.copy()
            if search:
                display_df = display_df[display_df['Style_No'].astype(str).str.contains(search, case=False) | 
                                        display_df['Bag_No'].astype(str).str.contains(search, case=False)]
            st.dataframe(display_df, use_container_width=True, hide_index=True)

        elif report_choice == "🔍 Bag History Report":
            st.subheader("🔍 Bag History Report")
            search_bag = st.text_input("Enter Bag Number to Track").strip()
            if search_bag:
                m_data = df[df['Bag_No'].astype(str) == search_bag]
                if not m_data.empty:
                    row = m_data.iloc[0]
                    st.markdown('<div class="section-head">Bag Details</div>', unsafe_allow_html=True)
                    c1, c2 = st.columns([2, 1])
                    c1.markdown(f"### Bag NO: `{search_bag}`")
                    status = row.get('Final_VZ_Status', 'N/A')
                    c2.warning(f"**Status:** {status}")
                    colA, colB = st.columns(2)
                    with colA:
                        st.write(f"1. **Customer:** {get_val(row.get('Customer'))}", unsafe_allow_html=True)
                        st.write(f"2. **Type:** {get_val(row.get('Order_Type'))}", unsafe_allow_html=True)
                    with colB:
                        st.write(f"3. **Metal:** {get_val(row.get(metal_col))}g", unsafe_allow_html=True)
                        st.write(f"4. **Dia Cts:** {get_val(row.get(dia_col))}", unsafe_allow_html=True)
                    show_history_tables(df_pre, "PRE-FINISH", search_bag, "PRE")
                    show_history_tables(df_post, "POST-FINISH", search_bag, "POST")
                else: st.error("Bag number not found.")

        elif report_choice == "📊 Metal Requirements":
            st.subheader("📊 Metal Requirements")
            df[metal_col] = pd.to_numeric(df[metal_col], errors='coerce').fillna(0)
            p_df = df[(df['Metal_Issue_Date'].isna()) | (df['Final_VZ_Status'] == "METAL PENDING")].copy()
            
            def create_metal_card(data, label):
                summary = data.groupby('Customer').agg({'Bag_No': 'count', metal_col: 'sum'})
                summary['Metal 18kt'] = summary[metal_col].apply(std_round)
                summary['Pure'] = (summary['Metal 18kt'] * 0.76).apply(std_round)
                c1, c2 = st.columns(2)
                c1.metric(f"{label} Bags", summary['Bag_No'].sum())
                c2.metric(f"18kt Total", f"{summary['Metal 18kt'].sum()}g")
                st.table(summary[['Bag_No', 'Metal 18kt', 'Pure']].rename(columns={'Bag_No': 'Qty'}))

            st.info("👤 CUSTOMER ORDERS")
            c_df = p_df[p_df['Order_Type'].str.contains('CUSTOMER', case=False, na=False)]
            if not c_df.empty: create_metal_card(c_df, "Cust")
            st.warning("📦 STOCK ORDERS")
            s_df = p_df[p_df['Order_Type'].str.contains('STOCK', case=False, na=False)]
            if not s_df.empty: create_metal_card(s_df, "Stock")

        elif report_choice == "📋 CSR":
            st.subheader("📋 Customer Status Report")
            status_seq = {"SEQUENCE": 0, "ENGRAVING/HUID": 1, "IGI": 2, "ON HAND": 3, "FINAL QC": 4, "SETTING QC OK": 5, "SETTING": 6, "GHAT OK": 7, "CASTING": 8, "METAL ISSUED": 9, "METAL PENDING": 10}
            csr_df = df.copy()
            csr_df[metal_col] = pd.to_numeric(csr_df[metal_col], errors='coerce').fillna(0)
            csr_df[dia_col] = pd.to_numeric(csr_df[dia_col], errors='coerce').fillna(0)
            csr_df['Seq'] = csr_df['Final_VZ_Status'].map(status_seq).fillna(99)
            
            for cust in sorted(csr_df['Customer'].unique()):
                with st.expander(f"👤 {cust}"):
                    cust_data = csr_df[csr_df['Customer'] == cust]
                    summary = cust_data.groupby(['Final_VZ_Status', 'Seq']).agg({'Bag_No': 'count', metal_col: 'sum', dia_col: 'sum'}).reset_index().sort_values('Seq')
                    summary['Metal 18kt'] = summary[metal_col].apply(std_round)
                    summary['Dia Cts'] = summary[dia_col].map('{:,.2f}'.format)
                    st.table(summary[['Final_VZ_Status', 'Bag_No', 'Metal 18kt', 'Dia Cts']].rename(columns={'Final_VZ_Status': 'Status', 'Bag_No': 'Qty'}))
