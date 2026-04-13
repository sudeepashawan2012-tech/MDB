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
        creds = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
        client = bigquery.Client(credentials=creds, project=creds.project_id)
        query = "SELECT * FROM `jewelry-sql-system.workshop_data.master_inventory`"
        df = client.query(query).to_dataframe()
        # Clean column names
        df.columns = [c.strip().replace(' ', '_').replace('.', '') for c in df.columns] 
        return df
    except Exception as e:
        st.error(f"SQL Error: {e}")
        return None

# --- HISTORY FETCH FUNCTION ---
@st.cache_data(ttl=300)
def fetch_history(urls_tuple):
    try:
        frames = []
        for url in urls_tuple:
            # Skipping 1 row as per your previous structure
            tmp = pd.read_csv(url, skiprows=1, header=None)
            frames.append(tmp)
        df_hist = pd.concat(frames, ignore_index=True)
        # Naming columns for Movement logic: [Date, Time, Process, Bag_No, Status]
        # Adjust index numbers below based on your CSV structure
        return df_hist
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
        # Pre-process numeric columns
        df['Metal'] = pd.to_numeric(df['Metal'], errors='coerce').fillna(0)
        df['Dia_Cts'] = pd.to_numeric(df['Dia_Cts'], errors='coerce').fillna(0)

        # 1. HOME
        if report_choice == "🏠 Home":
            st.subheader("🔍 Search Inventory")
            search = st.text_input("Search Style/Bag...", placeholder="Type here...")
            display_df = df.copy()
            if search:
                display_df = display_df[display_df['Style_No'].astype(str).str.contains(search, case=False) | 
                                        display_df['Bag_No'].astype(str).str.contains(search, case=False)]
            st.dataframe(display_df, use_container_width=True, hide_index=True)

        # 2. BAG HISTORY REPORT
        elif report_choice == "🔍 Bag History Report":
            st.subheader("🔍 Bag History Report")
            search_bag = st.text_input("Enter Bag Number").strip()
            
            if search_bag:
                m_data = df[df['Bag_No'].astype(str) == search_bag]
                if not m_data.empty:
                    row = m_data.iloc[0]
                    
                    # Top Section
                    c1, c2 = st.columns([3, 2])
                    with c1: st.markdown(f"### Bag No: `{search_bag}`")
                    with c2: st.subheader(f"Status: {row.get('Final_VZ_Status', 'X')}")
                    
                    st.divider()
                    
                    # Row 1: Core Details
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown(f"1. **Customer:** {get_val(row.get('Customer'))}", unsafe_allow_html=True)
                        st.markdown(f"2. **Karigar:** {get_val(row.get('Karigar'))}", unsafe_allow_html=True)
                        st.markdown(f"3. **Metal Wt 18kt:** {std_round(row.get('Metal'))}g", unsafe_allow_html=True)
                        st.markdown(f"4. **Dia Cts:** {get_val(row.get('Dia_Cts'))}", unsafe_allow_html=True)
                    with col2:
                        st.markdown(f"**Metal Issue Dt:** {get_val(row.get('Metal_Issue_Date'), True)}", unsafe_allow_html=True)
                        st.markdown(f"**Dia Issue Dt:** {get_val(row.get('Dia_Issue_Date'), True)}", unsafe_allow_html=True)
                        st.markdown(f"**Finish Date:** {get_val(row.get('Finish_Date'), True)}", unsafe_allow_html=True)
                        st.markdown(f"**IGI Date:** {get_val(row.get('IGI_Date'), True)}", unsafe_allow_html=True)

                    # Production Stages
                    st.divider()
                    st.write("### Production Stages")
                    sc1, sc2, sc3 = st.columns(3)
                    with sc1:
                        st.info("**Ghat QC**")
                        st.write(f"Name: {get_val(row.get('Ghat_QC_Name'))}", unsafe_allow_html=True)
                        st.write(f"Wt: {get_val(row.get('Ghat_Wt'))}", unsafe_allow_html=True)
                        st.write(f"Date: {get_val(row.get('Ghat_Date'), True)}", unsafe_allow_html=True)
                    with sc2:
                        st.success("**Setting QC**")
                        st.write(f"Name: {get_val(row.get('Setting_QC_Name'))}", unsafe_allow_html=True)
                        st.write(f"Wt: {get_val(row.get('Setting_Wt'))}", unsafe_allow_html=True)
                        st.write(f"Date: {get_val(row.get('Setting_Date'), True)}", unsafe_allow_html=True)
                    with sc3:
                        st.warning("**Final QC**")
                        st.write(f"Name: {get_val(row.get('Final_QC_Name'))}", unsafe_allow_html=True)
                        st.write(f"Wt: {get_val(row.get('Final_Wt'))}", unsafe_allow_html=True)
                        st.write(f"Remark: {get_val(row.get('Final_QC_Remark'))}", unsafe_allow_html=True)

                    # MOVEMENT LOGS (PRE & POST)
                    st.divider()
                    st.subheader("PRE FINISH MOVEMENT")
                    if df_pre is not None:
                        # Assuming structure: [0:Date, 1:Time, 2:Process, 3:BagNo, 4:Type(In/Out)]
                        bag_hist = df_pre[df_pre[3].astype(str) == search_bag]
                        if not bag_hist.empty:
                            st.table(bag_hist[[0, 1, 2, 4]].rename(columns={0:'Date', 1:'Time', 2:'Process', 4:'Movement'}))
                        else: st.write("No Pre-finish movement recorded.")

                    st.subheader("POST FINISH MOVEMENT")
                    if df_post is not None:
                        bag_post = df_post[df_post[3].astype(str) == search_bag]
                        if not bag_post.empty:
                            st.table(bag_post[[0, 1, 2, 4]].rename(columns={0:'Date', 1:'Time', 2:'Process', 4:'Movement'}))
                        else: st.write("No Post-finish movement recorded.")
                else:
                    st.error("Bag not found.")

        # 3. METAL REQUIREMENTS
        elif report_choice == "📊 Metal Requirements":
            st.subheader("📊 Metal Requirements (Pending Issue)")
            
            # CRITERIA: Metal Issue Date is blank AND Status is NOT "HOLD" or "CANCEL"
            exclude_status = ["HOLD", "CANCEL"]
            mask = (df['Metal_Issue_Date'].isna() | (df['Metal_Issue_Date'] == "")) & (~df['Final_VZ_Status'].isin(exclude_status))
            pending_df = df[mask].copy()

            def metal_report_section(title, data):
                st.markdown(f"### {title}")
                if not data.empty:
                    summary = data.groupby('Customer').agg({'Bag_No':'count', 'Metal':'sum', 'Dia_Cts':'sum'}).reset_index()
                    summary.columns = ['Customer Code', 'Bag Qty', 'Metal 18kt', 'Dia Cts']
                    summary['Metal 18kt'] = summary['Metal 18kt'].apply(std_round)
                    st.table(summary)
                    # Grand Total for Section
                    st.write(f"**Subtotal:** {summary['Bag Qty'].sum()} Bags | {summary['Metal 18kt'].sum()}g Metal | {summary['Dia Cts'].sum():.2f} Cts")
                else: st.write("No pending metal requirements.")

            # Customer Orders
            c_data = pending_df[pending_df['Order_Type'].str.contains('CUSTOMER', case=False, na=False)]
            metal_report_section("👤 CUSTOMER ORDERS", c_data)
            
            # Stock Orders
            s_data = pending_df[pending_df['Order_Type'].str.contains('STOCK', case=False, na=False)]
            metal_report_section("📦 STOCK ORDERS", s_data)

        # 4. CSR
        elif report_choice == "📋 CSR":
            st.subheader("📋 Customer Status Report")
            
            # Explicit Sequence Mapping
            status_seq = {
                "SEQUENCE": 0, "ENGRAVING/HUID": 1, "IGI": 2, "ON HAND": 3, 
                "FINAL QC": 4, "SETTING QC OK": 5, "SETTING": 6, "GHAT OK": 7, 
                "CASTING": 8, "METAL ISSUED": 9, "METAL PENDING": 10, 
                "HOLD": 12, "CANCEL": 13
            }
            
            csr_df = df.copy()
            csr_df['Seq'] = csr_df['Final_VZ_Status'].map(status_seq).fillna(99)

            for cust in sorted(csr_df['Customer'].unique()):
                with st.expander(f"👤 {cust}"):
                    cust_data = csr_df[csr_df['Customer'] == cust]
                    summary = cust_data.groupby(['Final_VZ_Status', 'Seq']).agg({
                        'Bag_No': 'count', 
                        'Metal': 'sum', 
                        'Dia_Cts': 'sum'
                    }).reset_index().sort_values('Seq')
                    
                    # Formatting
                    summary['Metal 18kt'] = summary['Metal'].apply(std_round)
                    summary['Dia Cts'] = summary['Dia_Cts'].apply(lambda x: f"{x:.2f}")
                    
                    # Final Display
                    display_summary = summary[['Final_VZ_Status', 'Bag_No', 'Metal 18kt', 'Dia Cts']].rename(
                        columns={'Final_VZ_Status': 'Status', 'Bag_No': 'Bag Qty'}
                    )
                    st.table(display_summary)
                    st.markdown(f"**TOTAL:** `{summary['Bag_No'].sum()}` Bags | `{summary['Metal 18kt'].sum()}g` 18kt | `{summary['Dia_Cts'].sum():.2f}` Cts")
