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
        # Using your exact table ID
        query = "SELECT * FROM `jewelry-sql-system.workshop_data.master_inventory`"
        df = client.query(query).to_dataframe()
        
        # Standardize columns to match the code logic
        # This removes trailing spaces and replaces spaces with underscores
        df.columns = [c.strip().replace(' ', '_').replace('.', '').replace('/', '') for c in df.columns] 
        return df
    except Exception as e:
        st.error(f"SQL Error: {e}")
        return None

# --- HISTORY FETCH FUNCTION ---
@st.cache_data(ttl=300)
def fetch_history(urls_tuple):
    try:
        frames = [pd.read_csv(url, skiprows=1, header=None) for url in urls_tuple]
        df_hist = pd.concat(frames, ignore_index=True)
        return df_hist
    except:
        return None

# --- HELPER FUNCTIONS ---
def get_val(val, is_date=False):
    # If value is NaN, empty, or "pending", return Red Bold X
    if pd.isna(val) or str(val).strip().lower() in ['nan', '', 'none', 'pending', 'false']:
        return '<span style="color: #ff4b4b; font-weight: bold;">X</span>'
    if is_date:
        try:
            # Try parsing date, if it fails return as is
            return pd.to_datetime(val).strftime('%d/%m/%Y')
        except:
            return str(val)
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
    # RUN APP
    df = fetch_master_from_sql()
    
    # Pre-finish and Post-finish movement links
    PRE_URLS = (
        "https://docs.google.com/spreadsheets/d/e/2PACX-1vRk0Sr33sugG2FtNtdqqk11u8b2aGKGniTN1n2qcWiOCg1W0Vi5JzqLWZdu1DWVUfpP2baURyOn4qo7/pub?gid=449683950&single=true&output=csv",
        "https://docs.google.com/spreadsheets/d/e/2PACX-1vRk0Sr33sugG2FtNtdqqk11u8b2aGKGniTN1n2qcWiOCg1W0Vi5JzqLWZdu1DWVUfpP2baURyOn4qo7/pub?gid=1700828894&single=true&output=csv"
    )
    POST_URLS = (
        "https://docs.google.com/spreadsheets/d/e/2PACX-1vRk0Sr33sugG2FtNtdqqk11u8b2aGKGniTN1n2qcWiOCg1W0Vi5JzqLWZdu1DWVUfpP2baURyOn4qo7/pub?gid=1461249957&single=true&output=csv",
        "https://docs.google.com/spreadsheets/d/e/2PACX-1vRk0Sr33sugG2FtNtdqqk11u8b2aGKGniTN1n2qcWiOCg1W0Vi5JzqLWZdu1DWVUfpP2baURyOn4qo7/pub?gid=163646832&single=true&output=csv"
    )
    
    df_pre = fetch_history(PRE_URLS)
    df_post = fetch_history(POST_URLS)

    # SIDEBAR
    st.sidebar.title("💼 ADMIN")
    report_choice = st.sidebar.selectbox("GO TO:", ["🏠 Home", "🔍 Bag History Report", "📊 Metal Requirements", "📋 CSR"])
    
    if st.sidebar.button("Logout"):
        del st.session_state["password_correct"]
        st.rerun()

    if df is not None:
        # Pre-convert numeric columns for calculations
        # Using column names from the provided CSV structure
        df['METAL_18KT_WT'] = pd.to_numeric(df['METAL_18KT_WT'], errors='coerce').fillna(0)
        df['DIA_CTS'] = pd.to_numeric(df['DIA_CTS'], errors='coerce').fillna(0)

        # 1. HOME
        if report_choice == "🏠 Home":
            st.subheader("🔍 Search Inventory")
            search = st.text_input("Search Style/Bag...", placeholder="Type here...")
            display_df = df.copy()
            if search:
                display_df = display_df[display_df['STYLE_NO'].astype(str).str.contains(search, case=False) | 
                                        display_df['BAG_NO'].astype(str).str.contains(search, case=False)]
            st.dataframe(display_df, use_container_width=True, hide_index=True)

        # 2. BAG HISTORY REPORT
        elif report_choice == "🔍 Bag History Report":
            st.subheader("🔍 Bag History Report")
            search_bag = st.text_input("Enter Bag Number").strip()
            if search_bag:
                m_data = df[df['BAG_NO'].astype(str) == search_bag]
                if not m_data.empty:
                    row = m_data.iloc[0]
                    
                    # Top Header
                    c_head1, c_head2 = st.columns([2, 1])
                    c_head1.markdown(f"### Bag No: `{search_bag}`")
                    c_head2.markdown(f"### Current Status: `{row.get('CURRENT_STATUS', 'X')}`")
                    
                    st.divider()
                    
                    # Section 1: 4 Details + Dates
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown(f"1. **Customer:** {get_val(row.get('CUSTOMER'))}", unsafe_allow_html=True)
                        st.markdown(f"2. **Karigar:** {get_val(row.get('KARIGAR'))}", unsafe_allow_html=True)
                        st.markdown(f"3. **Metal Wt 18kt:** {std_round(row.get('METAL_18KT_WT'))}g", unsafe_allow_html=True)
                        st.markdown(f"4. **Dia Cts:** {get_val(row.get('DIA_CTS'))}", unsafe_allow_html=True)
                    with col2:
                        st.markdown(f"**Metal Issue Dt:** {get_val(row.get('METAL_ISSUE_DATE'), True)}", unsafe_allow_html=True)
                        st.markdown(f"**Dia Issue Dt:** {get_val(row.get('DIA_ISSUE_DATE'), True)}", unsafe_allow_html=True)
                        st.markdown(f"**Finish Date:** {get_val(row.get('FINISH_DATE'), True)}", unsafe_allow_html=True)
                        st.markdown(f"**IGI Date:** {get_val(row.get('IGI_DATE'), True)}", unsafe_allow_html=True)

                    # Section 2: Production Stages
                    st.divider()
                    
                    # Ghat
                    st.markdown("#### GHAT QC")
                    g1, g2, g3 = st.columns(3)
                    g1.write(f"**Person Name:** {get_val(row.get('GHAT_QC'))}", unsafe_allow_html=True)
                    g2.write(f"**Ghat Wt:** {get_val(row.get('GHAT_WT'))}", unsafe_allow_html=True)
                    g3.write(f"**Ghat Date:** {get_val(row.get('GHAT_DATE'), True)}", unsafe_allow_html=True)
                    
                    # Colourstone
                    st.markdown("#### COLOURSTONE")
                    cs1, cs2 = st.columns(2)
                    with cs1:
                        st.write(f"**C/S 1st Issuer:** {get_val(row.get('CS_1ST_ISSUER'))}", unsafe_allow_html=True)
                        st.write(f"**Qty:** {get_val(row.get('CS_1ST_ISSUE_QTY'))}", unsafe_allow_html=True)
                        st.write(f"**Date:** {get_val(row.get('CS_1ST_ISSUE_DATE'), True)}", unsafe_allow_html=True)
                    with cs2:
                        st.write(f"**C/S 2nd Issuer:** {get_val(row.get('CS_2ND_ISSUER'))}", unsafe_allow_html=True)
                        st.write(f"**Qty:** {get_val(row.get('CS_2ND_ISSUE_QTY'))}", unsafe_allow_html=True)
                        st.write(f"**Date:** {get_val(row.get('CS_2ND_ISSUE_DATE'), True)}", unsafe_allow_html=True)

                    # Setting
                    st.markdown("#### SETTING QC")
                    s1, s2, s3 = st.columns(3)
                    s1.write(f"**Person Name:** {get_val(row.get('SETTING_QC'))}", unsafe_allow_html=True)
                    s2.write(f"**Setting Wt:** {get_val(row.get('SETTING_WT'))}", unsafe_allow_html=True)
                    s3.write(f"**Setting Date:** {get_val(row.get('SETTING_DATE'), True)}", unsafe_allow_html=True)

                    # Final QC
                    st.markdown("#### FINAL QC")
                    f1, f2, f3, f4 = st.columns(4)
                    f1.write(f"**Person Name:** {get_val(row.get('FINAL_QC'))}", unsafe_allow_html=True)
                    f2.write(f"**Final Wt:** {get_val(row.get('FINAL_WT'))}", unsafe_allow_html=True)
                    f3.write(f"**Remark:** {get_val(row.get('FINAL_QC_REMARK'))}", unsafe_allow_html=True)
                    f4.write(f"**Date:** {get_val(row.get('FINAL_QC_DATE'), True)}", unsafe_allow_html=True)

                    # Section 3: Movement Tables
                    st.divider()
                    
                    # PRE-FINISH MOVEMENT
                    st.subheader("PRE FINISH MOVEMENT")
                    if df_pre is not None:
                        # Index 0: Date, 1: Time, 2: Process, 3: BagNo, 4: In/Out
                        pre_data = df_pre[df_pre[3].astype(str) == search_bag].copy()
                        if not pre_data.empty:
                            col_a, col_b = st.columns(2)
                            with col_a:
                                st.write("**INWARD**")
                                st.table(pre_data[pre_data[4].str.contains('IN', case=False, na=False)][[0, 1, 2]].rename(columns={0:'DATE', 1:'TIME', 2:'PROCESS'}))
                            with col_b:
                                st.write("**OUTWARD**")
                                st.table(pre_data[pre_data[4].str.contains('OUT', case=False, na=False)][[0, 1, 2]].rename(columns={0:'DATE', 1:'TIME', 2:'PROCESS'}))
                        else: st.write("No record found.")

                    # POST-FINISH MOVEMENT
                    st.subheader("POST FINISH MOVEMENT")
                    if df_post is not None:
                        post_data = df_post[df_post[3].astype(str) == search_bag].copy()
                        if not post_data.empty:
                            col_a, col_b = st.columns(2)
                            with col_a:
                                st.write("**INWARD**")
                                st.table(post_data[post_data[4].str.contains('IN', case=False, na=False)][[0, 1, 2]].rename(columns={0:'DATE', 1:'TIME', 2:'PROCESS'}))
                            with col_b:
                                st.write("**OUTWARD**")
                                st.table(post_data[post_data[4].str.contains('OUT', case=False, na=False)][[0, 1, 2]].rename(columns={0:'DATE', 1:'TIME', 2:'PROCESS'}))
                        else: st.write("No record found.")
                else:
                    st.error("Bag not found.")

        # 3. METAL REQUIREMENTS
        elif report_choice == "📊 Metal Requirements":
            st.subheader("📊 Metal Requirements (Pending Issue)")
            
            # Logic: Metal Issue Date is blank + Status NOT in [HOLD, CANCEL]
            exclude_status = ["HOLD", "CANCEL"]
            mask = (df['METAL_ISSUE_DATE'].isna() | (df['METAL_ISSUE_DATE'] == "")) & (~df['CURRENT_STATUS'].isin(exclude_status))
            pending_df = df[mask].copy()

            def display_metal_table(title, data):
                st.markdown(f"### {title}")
                if not data.empty:
                    summary = data.groupby('CUSTOMER').agg({'BAG_NO':'count', 'METAL_18KT_WT':'sum', 'DIA_CTS':'sum'}).reset_index()
                    summary.columns = ['Customer Code', 'Bag Qty', 'Metal 18kt', 'Dia Cts']
                    summary['Metal 18kt'] = summary['Metal 18kt'].apply(std_round)
                    st.table(summary)
                    st.write(f"**Subtotal:** `{summary['Bag Qty'].sum()}` Bags | `{summary['Metal 18kt'].sum()}g` Metal | `{summary['Dia Cts'].sum():.2f}` Cts")
                else: st.info("No items pending.")

            # Customer Orders
            c_df = pending_df[pending_df['ORDER_TYPE'].str.contains('CUSTOMER', case=False, na=False)]
            display_metal_table("👤 CUSTOMER ORDERS", c_df)
            
            # Stock Orders
            s_df = pending_df[pending_df['ORDER_TYPE'].str.contains('STOCK', case=False, na=False)]
            display_metal_table("📦 STOCK ORDERS", s_df)

        # 4. CSR
        elif report_choice == "📋 CSR":
            st.subheader("📋 Customer Status Report")
            status_seq = {
                "SEQUENCE": 0, "ENGRAVING/HUID": 1, "IGI": 2, "ON HAND": 3, "FINAL QC": 4,
                "SETTING QC OK": 5, "SETTING": 6, "GHAT OK": 7, "CASTING": 8, 
                "METAL ISSUED": 9, "METAL PENDING": 10, "HOLD": 12, "CANCEL": 13
            }
            
            csr_df = df.copy()
            csr_df['Seq'] = csr_df['CURRENT_STATUS'].map(status_seq).fillna(99)

            for cust in sorted(csr_df['CUSTOMER'].unique()):
                with st.expander(f"👤 {cust}"):
                    cust_data = csr_df[csr_df['CUSTOMER'] == cust]
                    summary = cust_data.groupby(['CURRENT_STATUS', 'Seq']).agg({
                        'BAG_NO': 'count', 
                        'METAL_18KT_WT': 'sum', 
                        'DIA_CTS': 'sum'
                    }).reset_index().sort_values('Seq')
                    
                    summary['Metal 18kt'] = summary['METAL_18KT_WT'].apply(std_round)
                    summary['Dia Cts'] = summary['DIA_CTS'].apply(lambda x: f"{x:.2f}")
                    
                    display_summary = summary[['CURRENT_STATUS', 'BAG_NO', 'Metal 18kt', 'Dia Cts']].rename(
                        columns={'CURRENT_STATUS': 'Status', 'BAG_NO': 'Bag Qty'}
                    )
                    st.table(display_summary)
                    st.markdown(f"**TOTAL:** `{summary['BAG_NO'].sum()}` Bags | `{summary['Metal 18kt'].sum()}g` 18kt | `{summary['DIA_CTS'].sum():.2f}` Cts")
