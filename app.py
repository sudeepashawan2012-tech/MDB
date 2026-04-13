import streamlit as st
import pandas as pd
import numpy as np
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
        
        # --- ROBUST COLUMN CLEANING ---
        # 1. Strip spaces 2. Uppercase for consistency 3. Replace internal spaces with underscore
        df.columns = [str(c).strip().upper().replace(' ', '_').replace('.', '') for c in df.columns]
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
def get_val(val, is_date=False):
    if pd.isna(val) or str(val).strip().lower() in ['nan', '', 'none', 'pending']:
        return '<span style="color: #ff4b4b; font-weight: bold;">X</span>'
    if is_date:
        try: return pd.to_datetime(val).strftime('%d/%m/%Y')
        except: return str(val)
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
    df = fetch_master_from_sql()
    
    # Pre/Post History URLs
    LIVE_URLS = ("https://docs.google.com/spreadsheets/d/e/2PACX-1vRk0Sr33sugG2FtNtdqqk11u8b2aGKGniTN1n2qcWiOCg1W0Vi5JzqLWZdu1DWVUfpP2baURyOn4qo7/pub?gid=449683950&single=true&output=csv", "https://docs.google.com/spreadsheets/d/e/2PACX-1vRk0Sr33sugG2FtNtdqqk11u8b2aGKGniTN1n2qcWiOCg1W0Vi5JzqLWZdu1DWVUfpP2baURyOn4qo7/pub?gid=1700828894&single=true&output=csv")
    POST_URLS = ("https://docs.google.com/spreadsheets/d/e/2PACX-1vRk0Sr33sugG2FtNtdqqk11u8b2aGKGniTN1n2qcWiOCg1W0Vi5JzqLWZdu1DWVUfpP2baURyOn4qo7/pub?gid=1461249957&single=true&output=csv", "https://docs.google.com/spreadsheets/d/e/2PACX-1vRk0Sr33sugG2FtNtdqqk11u8b2aGKGniTN1n2qcWiOCg1W0Vi5JzqLWZdu1DWVUfpP2baURyOn4qo7/pub?gid=163646832&single=true&output=csv")
    
    df_pre = fetch_history(LIVE_URLS)
    df_post = fetch_history(POST_URLS)

    st.sidebar.title("💼 ADMIN")
    report_choice = st.sidebar.selectbox("GO TO:", ["🏠 Home", "🔍 Bag History Report", "📊 Metal Requirements", "📋 CSR"])
    
    if st.sidebar.button("Logout"):
        del st.session_state["password_correct"]
        st.rerun()

    if df is not None:
        # MAP COLUMN NAMES TO VARIABLE NAMES (Prevents KeyError)
        # We look for the most likely matches in your SQL table
        cols = {
            'BAG': next((c for c in df.columns if 'BAG_NO' in c or 'BAG_NUMBER' in c or c == 'BAG'), 'BAG_NO'),
            'METAL': next((c for c in df.columns if 'METAL' in c and 'DATE' not in c), 'METAL'),
            'DIA': next((c for c in df.columns if 'DIA_CTS' in c or 'DIA_WT' in c or c == 'DIA'), 'DIA_CTS'),
            'STATUS': next((c for c in df.columns if 'STATUS' in c), 'FINAL_VZ_STATUS'),
            'CUST': next((c for c in df.columns if 'CUSTOMER' in c), 'CUSTOMER'),
            'ORDER_TYPE': next((c for c in df.columns if 'ORDER_TYPE' in c), 'ORDER_TYPE'),
            'METAL_DT': next((c for c in df.columns if 'METAL_ISSUE_DATE' in c or 'METAL_DATE' in c), 'METAL_ISSUE_DATE')
        }

        # Safe Numeric Conversion
        df[cols['METAL']] = pd.to_numeric(df[cols['METAL']], errors='coerce').fillna(0)
        df[cols['DIA']] = pd.to_numeric(df[cols['DIA']], errors='coerce').fillna(0)

        # 1. HOME
        if report_choice == "🏠 Home":
            st.subheader("🔍 Search Inventory")
            search = st.text_input("Search Style/Bag...", placeholder="Type here...")
            display_df = df.copy()
            if search:
                display_df = display_df[display_df[cols['BAG']].astype(str).str.contains(search, case=False)]
            st.dataframe(display_df, use_container_width=True, hide_index=True)

        # 2. BAG HISTORY REPORT
        elif report_choice == "🔍 Bag History Report":
            st.subheader("🔍 Bag History Report")
            search_bag = st.text_input("Search Bag Number").strip()
            if search_bag:
                m_data = df[df[cols['BAG']].astype(str) == search_bag]
                if not m_data.empty:
                    row = m_data.iloc[0]
                    st.markdown(f"### Bag No: `{search_bag}` | Status: `{row.get(cols['STATUS'], 'X')}`")
                    
                    st.divider()
                    colA, colB = st.columns(2)
                    with colA:
                        st.markdown(f"1. **Customer:** {get_val(row.get(cols['CUST']))}", unsafe_allow_html=True)
                        st.markdown(f"2. **Karigar:** {get_val(row.get('KARIGAR'))}", unsafe_allow_html=True)
                        st.markdown(f"3. **Metal Wt 18kt:** {std_round(row.get(cols['METAL']))}", unsafe_allow_html=True)
                        st.markdown(f"4. **Dia Cts:** {get_val(row.get(cols['DIA']))}", unsafe_allow_html=True)
                    with colB:
                        st.markdown(f"**Metal Issue Dt:** {get_val(row.get(cols['METAL_DT']), True)}", unsafe_allow_html=True)
                        st.markdown(f"**Finish Date:** {get_val(row.get('FINISH_DATE'), True)}", unsafe_allow_html=True)

                    # Movement History
                    st.divider()
                    st.subheader("PRE FINISH MOVEMENT")
                    if df_pre is not None:
                        p_hist = df_pre[df_pre[3].astype(str) == search_bag]
                        st.table(p_hist[[0, 1, 2, 4]].rename(columns={0:'DATE', 1:'TIME', 2:'PROCESS', 4:'STATUS'}))
                else: st.error("Bag Not Found")

        # 3. METAL REQUIREMENTS
        elif report_choice == "📊 Metal Requirements":
            st.subheader("📊 Metal Requirements")
            exclude = ["HOLD", "CANCEL"]
            mask = (df[cols['METAL_DT']].isna() | (df[cols['METAL_DT']] == "")) & (~df[cols['STATUS']].isin(exclude))
            pending_df = df[mask].copy()

            for o_type in ["CUSTOMER", "STOCK"]:
                st.markdown(f"#### {o_type} ORDERS")
                sub_data = pending_df[pending_df[cols['ORDER_TYPE']].str.contains(o_type, case=False, na=False)]
                if not sub_data.empty:
                    summ = sub_data.groupby(cols['CUST']).agg({cols['BAG']:'count', cols['METAL']:'sum', cols['DIA']:'sum'}).reset_index()
                    summ.columns = ['Customer Code', 'Bag Qty', 'Metal 18kt', 'Dia Cts']
                    summ['Metal 18kt'] = summ['Metal 18kt'].apply(std_round)
                    st.table(summ)
                else: st.write("No Pending Items.")

        # 4. CSR
        elif report_choice == "📋 CSR":
            st.subheader("📋 Customer Status Report")
            status_seq = {"SEQUENCE": 0, "ENGRAVING/HUID": 1, "IGI": 2, "ON HAND": 3, "FINAL QC": 4, "SETTING QC OK": 5, "SETTING": 6, "GHAT OK": 7, "CASTING": 8, "METAL ISSUED": 9, "METAL PENDING": 10, "HOLD": 12, "CANCEL": 13}
            
            csr_df = df.copy()
            csr_df['Seq'] = csr_df[cols['STATUS']].map(status_seq).fillna(99)

            for cust in sorted(csr_df[cols['CUST']].unique()):
                with st.expander(f"👤 {cust}"):
                    c_data = csr_df[csr_df[cols['CUST']] == cust]
                    summary = c_data.groupby([cols['STATUS'], 'Seq']).agg({cols['BAG']:'count', cols['METAL']:'sum', cols['DIA']:'sum'}).reset_index().sort_values('Seq')
                    summary['Metal 18kt'] = summary[cols['METAL']].apply(std_round)
                    st.table(summary[[cols['STATUS'], cols['BAG'], 'Metal 18kt', cols['DIA']]].rename(columns={cols['STATUS']:'Status', cols['BAG']:'Qty'}))
