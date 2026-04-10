import streamlit as st
import pandas as pd
import requests
import numpy as np

# 1. SETUP
st.set_page_config(page_title="MASTER DATABASE", layout="wide", initial_sidebar_state="collapsed")

# --- DATA SOURCE LINKS ---
MASTER_API_URL = "https://script.google.com/macros/s/AKfycbzJeiT_mTmPFVEFDqDZvnZeakdFVxUrGiOjtl-NBgGFHyi3HYLCO1648JSm7s2bW0A/exec"
LIVE_CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRk0Sr33sugG2FtNtdqqk11u8b2aGKGniTN1n2qcWiOCg1W0Vi5JzqLWZdu1DWVUfpP2baURyOn4qo7/pub?gid=449683950&single=true&output=csv"
ARCHIVE_CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRk0Sr33sugG2FtNtdqqk11u8b2aGKGniTN1n2qcWiOCg1W0Vi5JzqLWZdu1DWVUfpP2baURyOn4qo7/pub?gid=1700828894&single=true&output=csv"
POST_1 = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRk0Sr33sugG2FtNtdqqk11u8b2aGKGniTN1n2qcWiOCg1W0Vi5JzqLWZdu1DWVUfpP2baURyOn4qo7/pub?gid=1461249957&single=true&output=csv"
POST_2 = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRk0Sr33sugG2FtNtdqqk11u8b2aGKGniTN1n2qcWiOCg1W0Vi5JzqLWZdu1DWVUfpP2baURyOn4qo7/pub?gid=163646832&single=true&output=csv"

# Custom CSS for Minimalist UI
st.markdown("""
    <style>
    [data-testid="stMetric"] { background-color: rgba(125, 125, 125, 0.1); padding: 10px; border-radius: 8px; }
    .stTable { overflow-x: auto; }
    .section-head { background-color: #2e2e2e; color: #f0f0f0; padding: 8px 15px; border-radius: 5px; margin: 20px 0 10px 0; font-weight: bold; }
    .missing-data { color: #ff4b4b; font-weight: bold; }
    </style>
    """, unsafe_allow_html=True)

def check_password():
    if "password_correct" not in st.session_state:
        st.title("🔒 Master Structure & Reports")
        pwd = st.text_input("Workshop Password", type="password")
        if st.button("Login"):
            if pwd == "12345": 
                st.session_state["password_correct"] = True
                st.rerun()
            else: st.error("Invalid Password")
        return False
    return True

if check_password():
    
    @st.cache_data(ttl=60)
    def fetch_master():
        try:
            r = requests.get(MASTER_API_URL, timeout=15)
            return pd.DataFrame(r.json()).replace('', np.nan).dropna(subset=['Bag_No'], how='all')
        except: return None

    @st.cache_data(ttl=60)
    def fetch_history(urls):
        try:
            frames = []
            for url in urls:
                tdf = pd.read_csv(url, skiprows=1)
                tdf.columns = tdf.columns.str.strip()
                frames.append(tdf)
            combined = pd.concat(frames, ignore_index=True)
            combined['BAG NO'] = combined['BAG NO'].astype(str).str.strip()
            return combined
        except: return None

    df = fetch_master()
    df_pre = fetch_history([LIVE_CSV_URL, ARCHIVE_CSV_URL])
    df_post = fetch_history([POST_1, POST_2])

    def get_val(val):
        if pd.isna(val) or str(val).strip().lower() in ['nan', '', 'none', 'pending']:
            return '<span class="missing-data">X</span>'
        return str(val)

    def std_round(x):
        try: return int(float(x) + 0.5) if float(x) > 0 else 0
        except: return 0

    def show_clean_tables(hist_df, label, search_bag):
        st.markdown(f'<div class="section-head">{label} MOVEMENT</div>', unsafe_allow_html=True)
        if hist_df is not None:
            search_bag_str = str(search_bag).strip()
            moves = hist_df[hist_df['BAG NO'].astype(str).str.strip() == search_bag_str].copy()
            
            if not moves.empty:
                h_in, h_out = st.columns(2)
                
                with h_in:
                    st.info("Inward")
                    # Column 'I' is index 8 (A=0, B=1... I=8)
                    # We target Column I for Inward Purpose and Column H (Index 7) for Inward Date
                    try:
                        in_df = pd.DataFrame({
                            'Date': moves.iloc[:, 7],    # Column H: Inward Date
                            'Purpose': moves.iloc[:, 8]  # Column I: Inward Purpose
                        })
                        in_df['Purpose'] = in_df['Purpose'].fillna("N/A")
                        in_df = in_df.dropna(subset=['Date'])
                        st.table(in_df)
                    except:
                        st.warning("Inward Data Structure mismatch.")
                    
                with h_out:
                    st.error("Outward")
                    # Targeting Column J (Index 9) for Outward Date and Column K (Index 10) for Outward Purpose
                    try:
                        out_df = pd.DataFrame({
                            'Date': moves.iloc[:, 9],    # Column J
                            'Purpose': moves.iloc[:, 10] # Column K
                        })
                        out_df['Purpose'] = out_df['Purpose'].fillna("N/A")
                        out_df = out_df.dropna(subset=['Date'])
                        st.table(out_df)
                    except:
                        st.warning("Outward Data Structure mismatch.")
            else: 
                st.info(f"No {label} logs found.")

    st.sidebar.title("💼 ADMIN")
    report_choice = st.sidebar.selectbox("GO TO:", ["🏠 Home", "🔍 Bag History Report", "📊 Metal Requirements", "📋 CSR"])
    
    if st.sidebar.button("Logout"):
        del st.session_state["password_correct"]
        st.rerun()

    if df is not None:
        if report_choice == "🏠 Home":
            st.subheader("🔍 Search Inventory")
            search = st.text_input("Search Style/Bag...", placeholder="Type here...")
            display_df = df.copy()
            if search:
                display_df = display_df[display_df['Style_No'].astype(str).str.contains(search, case=False) | 
                                        display_df['Bag_No'].astype(str).str.contains(search, case=False)]
            st.dataframe(display_df, column_config={"Thumbnail_Link": st.column_config.ImageColumn("Preview")}, use_container_width=True, hide_index=True)

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
                    status = row.get('Final_VZ_Status', 'N/A')
                    c2.warning(f"**Status:** {status}")
                    
                    colA, colB = st.columns(2)
                    with colA:
                        st.write(f"1. **Customer:** {get_val(row.get('Customer'))}", unsafe_allow_html=True)
                        st.write(f"2. **Type:** {get_val(row.get('Order_Type'))}", unsafe_allow_html=True)
                        st.write(f"3. **Order Date:** {get_val(row.get('Order_Date'))}", unsafe_allow_html=True)
                        st.write(f"4. **Karigar:** {get_val(row.get('Karigar'))}", unsafe_allow_html=True)
                    with colB:
                        st.write(f"5. **Metal:** {get_val(row.get('Metal'))}g", unsafe_allow_html=True)
                        st.write(f"6. **Dia Cts:** {get_val(row.get('Dia_Cts'))}", unsafe_allow_html=True)
                        m_issue = row.get('Metal_Issue_Date')
                        if (pd.isna(m_issue) or str(m_issue).strip() == "") and status == "CASTING":
                            m_issue = "25/03/2026"
                        st.write(f"7. **Metal Issue:** {get_val(m_issue)}", unsafe_allow_html=True)
                        st.write(f"8. **Deliv. Date:** {get_val(row.get('Delivery_Date'))}", unsafe_allow_html=True)

                    show_clean_tables(df_pre, "PRE-FINISH", search_bag)
                    show_clean_tables(df_post, "POST-FINISH", search_bag)
                else: st.error("Bag number not found.")

        elif report_choice == "📊 Metal Requirements":
            st.subheader("📊 Metal Requirements")
            df['Metal'] = pd.to_numeric(df['Metal'], errors='coerce').fillna(0)
            p_df = df[(df['Metal_Issue_Date'].isna()) | (df['Final_VZ_Status'] == "METAL PENDING")].copy()
            
            def create_metal_card(data, label):
                summary = data.groupby('Customer').agg({'Bag_No': 'count', 'Metal': 'sum'})
                summary['Metal 18kt'] = summary['Metal'].apply(std_round)
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
            csr_df['Metal'] = pd.to_numeric(csr_df['Metal'], errors='coerce').fillna(0)
            csr_df['Dia_Cts'] = pd.to_numeric(csr_df['Dia_Cts'], errors='coerce').fillna(0)
            csr_df['Seq'] = csr_df['Final_VZ_Status'].map(status_seq).fillna(99)

            for cust in sorted(csr_df['Customer'].unique()):
                with st.expander(f"👤 {cust}"):
                    cust_data = csr_df[csr_df['Customer'] == cust]
                    summary = cust_data.groupby(['Final_VZ_Status', 'Seq']).agg({'Bag_No': 'count', 'Metal': 'sum', 'Dia_Cts': 'sum'}).reset_index().sort_values('Seq')
                    summary['Metal 18kt'] = summary['Metal'].apply(std_round)
                    summary['Dia Cts'] = summary['Dia_Cts'].map('{:,.2f}'.format)
                    st.table(summary[['Final_VZ_Status', 'Bag_No', 'Metal 18kt', 'Dia Cts']].rename(columns={'Final_VZ_Status': 'Status', 'Bag_No': 'Qty'}))
                    st.markdown(f"**TOTAL:** `{summary['Bag_No'].sum()}` Bags | `{summary['Metal 18kt'].sum()}g` 18kt | `{summary['Dia_Cts'].sum():.2f}` Dia Cts")
