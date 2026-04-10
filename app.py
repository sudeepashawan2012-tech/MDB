import streamlit as st
import pandas as pd
import requests
import numpy as np

# 1. SETUP & MOBILE-FRIENDLY CONFIG
st.set_page_config(page_title="MASTER DATABASE", layout="wide", initial_sidebar_state="collapsed")

# --- DATA SOURCE LINKS ---
MASTER_API_URL = "https://script.google.com/macros/s/AKfycbzJeiT_mTmPFVEFDqDZvnZeakdFVxUrGiOjtl-NBgGFHyi3HYLCO1648JSm7s2bW0A/exec"
LIVE_CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRk0Sr33sugG2FtNtdqqk11u8b2aGKGniTN1n2qcWiOCg1W0Vi5JzqLWZdu1DWVUfpP2baURyOn4qo7/pub?gid=449683950&single=true&output=csv"
ARCHIVE_CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRk0Sr33sugG2FtNtdqqk11u8b2aGKGniTN1n2qcWiOCg1W0Vi5JzqLWZdu1DWVUfpP2baURyOn4qo7/pub?gid=1700828894&single=true&output=csv"

# Custom CSS for Theme Compatibility
st.markdown("""
    <style>
    [data-testid="stMetric"] { background-color: rgba(125, 125, 125, 0.1); padding: 10px; border-radius: 8px; }
    .stTable { overflow-x: auto; }
    .st-emotion-cache-16idsys p { font-size: 1.1rem; }
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
            else:
                st.error("Invalid Password")
        return False
    return True

if check_password():
    
    # --- FETCHING FUNCTIONS ---
    @st.cache_data(ttl=60)
    def fetch_master_data():
        try:
            response = requests.get(MASTER_API_URL, timeout=15)
            raw_df = pd.DataFrame(response.json())
            if not raw_df.empty:
                return raw_df.replace('', np.nan).dropna(subset=['Bag_No', 'Customer'], how='all')
            return None
        except: return None

    @st.cache_data(ttl=60)
    def fetch_history_data():
        try:
            df_live = pd.read_csv(LIVE_CSV_URL, skiprows=1)
            df_archive = pd.read_csv(ARCHIVE_CSV_URL, skiprows=1)
            combined = pd.concat([df_live, df_archive], ignore_index=True)
            combined['BAG NO'] = combined['BAG NO'].astype(str).str.strip()
            combined['Date_Sort'] = pd.to_datetime(combined['INWARD DATE '], errors='coerce', dayfirst=True)
            return combined.sort_values('Date_Sort', ascending=True)
        except: return None

    df = fetch_master_data()
    df_hist = fetch_history_data()

    def std_round(x):
        try: return int(float(x) + 0.5) if float(x) > 0 else 0
        except: return 0

    # --- NAVIGATION ---
    st.sidebar.title("💼 ADMIN")
    report_choice = st.sidebar.selectbox("GO TO:", ["🏠 Home", "🔍 Bag History Report", "📊 Metal Requirements", "📋 CSR (Status Report)"])
    
    if st.sidebar.button("Logout"):
        del st.session_state["password_correct"]
        st.rerun()

    if df is not None:
        
        # 1. HOME / SEARCH
        if report_choice == "🏠 Home":
            st.subheader("🔍 Search Inventory")
            search = st.text_input("Search Style/Bag...", placeholder="Type here...")
            display_df = df.copy()
            if search:
                display_df = display_df[display_df['Style_No'].astype(str).str.contains(search, case=False) | 
                                        display_df['Bag_No'].astype(str).str.contains(search, case=False)]
            st.dataframe(display_df, column_config={"Thumbnail_Link": st.column_config.ImageColumn("Preview")}, use_container_width=True, hide_index=True)

        # 2. BAG HISTORY REPORT
        elif report_choice == "🔍 Bag History Report":
            st.subheader("🔍 Bag History Report")
            search_bag = st.text_input("Search Bag Number (e.g. 26/P/369)").strip()
            if search_bag:
                m_data = df[df['Bag_No'].astype(str) == search_bag]
                if not m_data.empty:
                    row = m_data.iloc[0]
                    c1, c2 = st.columns([2, 1])
                    c1.markdown(f"### Bag NO: `{search_bag}`")
                    c2.warning(f"**Current Status:** {row.get('Final_VZ_Status', 'N/A')}")
                    
                    st.markdown("---")
                    colA, colB = st.columns(2)
                    with colA:
                        st.write(f"1. **Customer name:** {row.get('Customer', 'N/A')}")
                        st.write(f"2. **Order Type:** {row.get('Order_Type', 'N/A')}")
                        st.write(f"3. **Order Date:** {row.get('Order_Date', 'N/A')}")
                        st.write(f"4. **Karigar:** {row.get('Karigar', 'N/A')}")
                        st.write(f"5. **Metal Colour:** {row.get('Metal_Colour', 'N/A')}")
                    with colB:
                        st.write(f"6. **Metal (Weight):** {row.get('Metal', '0')}g")
                        st.write(f"7. **Dia Cts:** {row.get('Dia_Cts', '0')} cts")
                        st.write(f"8. **Metal Issue Date:** {row.get('Metal_Issue_Date', 'Pending')}")
                        st.write(f"9. **Diamond Issue Date:** {row.get('Diamond_Date', 'Pending')}")
                        st.write(f"10. **Delivery Date:** {row.get('Delivery_Date', 'N/A')}")
                    
                    st.markdown("---")
                    if df_hist is not None:
                        moves = df_hist[df_hist['BAG NO'] == search_bag]
                        if not moves.empty:
                            h1, h2 = st.columns(2)
                            with h1:
                                st.info("**Inward Logs**")
                                in_l = moves[['INWARD DATE ', 'PURPOSE IN']].dropna(subset=['PURPOSE IN'])
                                st.table(in_l.rename(columns={'INWARD DATE ': 'Date', 'PURPOSE IN': 'Purpose In'}))
                            with h2:
                                st.error("**Outward Logs**")
                                out_l = moves[['OUTWARD DATE ', 'PURPOSE OUT']].dropna(subset=['PURPOSE OUT'])
                                st.table(out_l.rename(columns={'OUTWARD DATE ': 'Date', 'PURPOSE OUT': 'Purpose Out'}))
                        else: st.info("No movement logs found.")
                else: st.error("Bag not found in Master.")

        # 3. METAL REQUIREMENTS
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

        # 4. CSR
        elif report_choice == "📋 CSR (Status Report)":
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
