import streamlit as st
import pandas as pd
import requests
import numpy as np

# 1. SETUP & MOBILE-FRIENDLY CONFIG
st.set_page_config(page_title="JDS MASTER", layout="wide", initial_sidebar_state="collapsed")

# Custom CSS for Theme Compatibility (Dark/Light) and Mobile Padding
st.markdown("""
    <style>
    /* Make metrics look good on small screens */
    [data-testid="stMetric"] {
        background-color: rgba(125, 125, 125, 0.1);
        padding: 10px;
        border-radius: 8px;
    }
    /* Fix for mobile sidebar overlay */
    .st-emotion-cache-16idsys p { font-size: 1.1rem; }
    
    /* Ensure tables don't get cut off */
    .stTable { overflow-x: auto; }
    </style>
    """, unsafe_allow_html=True)

def check_password():
    if "password_correct" not in st.session_state:
        st.title("🔒 JDS Management")
        pwd = st.text_input("Workshop Password", type="password")
        if st.button("Login"):
            if pwd == "JDS2026":
                st.session_state["password_correct"] = True
                st.rerun()
            else:
                st.error("Invalid Password")
        return False
    return True

if check_password():
    API_URL = "https://script.google.com/macros/s/AKfycbzJeiT_mTmPFVEFDqDZvnZeakdFVxUrGiOjtl-NBgGFHyi3HYLCO1648JSm7s2bW0A/exec"

    @st.cache_data(ttl=60)
    def fetch_data():
        try:
            with st.spinner('📱 Loading Mobile Dashboard...'):
                response = requests.get(API_URL, timeout=15)
                raw_df = pd.DataFrame(response.json())
                if not raw_df.empty:
                    raw_df = raw_df.replace('', np.nan).dropna(subset=['Bag_No', 'Customer'], how='all')
                    return raw_df
                return None
        except:
            return None

    df = fetch_data()

    def std_round(x):
        return int(x + 0.5) if x > 0 else 0

    # --- TOP NAVIGATION (Better for Mobile than Sidebar) ---
    st.sidebar.title("💼 JDS ADMIN")
    report_choice = st.sidebar.selectbox("GO TO:", ["🏠 Home", "📊 Metal Requirements", "📋 CSR (Status Report)"])
    
    if st.sidebar.button("Logout"):
        del st.session_state["password_correct"]
        st.rerun()

    if df is not None:
        if report_choice == "📊 Metal Requirements":
            st.subheader("📊 Metal Requirements")
            pending_mask = (df['Metal_Issue_Date'].isna()) | (df['Final_VZ_Status'] == "METAL PENDING")
            pending_df = df[pending_mask].copy()
            pending_df['Metal'] = pd.to_numeric(pending_df['Metal'], errors='coerce').fillna(0)

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
                    
                    # Formatting Table
                    st.table(summary[['Final_VZ_Status', 'Bag_No', 'Metal 18kt', 'Dia Cts']].rename(columns={'Final_VZ_Status': 'Status', 'Bag_No': 'Qty'}))
                    
                    # TOTALS AT THE BOTTOM (Per your screenshot)
                    t_bags = summary['Bag_No'].sum()
                    t_metal = summary['Metal 18kt'].sum()
                    t_dia = summary['Dia_Cts'].sum()
                    
                    st.markdown(f"**TOTAL:** `{t_bags}` Bags | `{t_metal}g` 18kt | `{t_dia:.2f}` Dia Cts")

        else:
            st.subheader("🔍 Search Inventory")
            search = st.text_input("Search Style/Bag...", placeholder="Type here...")
            
            display_df = df.copy()
            if search:
                display_df = display_df[display_df['Style_No'].astype(str).str.contains(search, case=False) | display_df['Bag_No'].astype(str).str.contains(search, case=False)]
            
            st.dataframe(display_df, column_config={"Thumbnail_Link": st.column_config.ImageColumn("Preview")}, use_container_width=True, hide_index=True)
