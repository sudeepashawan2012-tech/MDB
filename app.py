import streamlit as st
import pandas as pd
import requests
import numpy as np

# 1. SETUP & SECURITY CONFIG
st.set_page_config(page_title="MASTER DATABASE | SECURE", layout="wide")

# CUSTOM CSS: Professional Spinner + Layout
st.markdown("""
    <style>
    .main { background-color: #f8f9fa; }
    .stMetric { background-color: #ffffff; padding: 15px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }
    /* This hides the "cooking" animations */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    </style>
    """, unsafe_allow_html=True)

# --- LOGIN FUNCTION ---
def check_password():
    """Returns True if the user had the correct password."""
    def password_entered():
        if st.session_state["password"] == "12345": # <--- SET YOUR PASSWORD HERE
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # don't store password
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        # First run, show input for password
        st.title("🔒 JDS Management System")
        st.text_input("Enter Workshop Password", type="password", on_change=password_entered, key="password")
        return False
    elif not st.session_state["password_correct"]:
        # Password incorrect, show input + error
        st.title("🔒 JDS Management System")
        st.text_input("Enter Workshop Password", type="password", on_change=password_entered, key="password")
        st.error("😕 Password incorrect")
        return False
    else:
        # Password correct
        return True

# --- START APP LOGIC ---
if check_password():
    
    API_URL = "https://script.google.com/macros/s/AKfycbzJeiT_mTmPFVEFDqDZvnZeakdFVxUrGiOjtl-NBgGFHyi3HYLCO1648JSm7s2bW0A/exec"

    @st.cache_data(ttl=60)
    def fetch_data():
        try:
            # Using a simple spinner instead of cake/balloons
            with st.spinner('🔄 Synchronizing Workshop Data...'):
                response = requests.get(API_URL, timeout=15)
                raw_df = pd.DataFrame(response.json())
                
                if not raw_df.empty:
                    # Clean ghost rows
                    raw_df = raw_df.replace('', np.nan)
                    raw_df = raw_df.dropna(subset=['Bag_No', 'Customer'], how='all')
                    return raw_df
                return None
        except Exception as e:
            st.error(f"Network Timeout: {e}")
            return None

    df = fetch_data()

    def std_round(x):
        return int(x + 0.5) if x > 0 else 0

    # --- SIDEBAR NAV ---
    with st.sidebar:
        st.title("💼 JDS ADMIN")
        report_choice = st.radio("NAVIGATION", ["🏠 Home / Search", "📊 Metal Requirements", "📋 CSR (Status Report)"])
        st.divider()
        if st.button("Logout"):
            st.session_state["password_correct"] = False
            st.rerun()

    # --- REPORTS ---
    if df is not None:
        if report_choice == "📊 Metal Requirements":
            st.header("📊 Pending Metal Requirements")
            
            pending_mask = (df['Metal_Issue_Date'].isna()) | (df['Final_VZ_Status'] == "METAL PENDING")
            pending_df = df[pending_mask].copy()
            pending_df['Metal'] = pd.to_numeric(pending_df['Metal'], errors='coerce').fillna(0)

            def create_exec_summary(data, title):
                summary = data.groupby('Customer').agg({'Bag_No': 'count', 'Metal': 'sum'})
                summary['Metal 18kt'] = summary['Metal'].apply(std_round)
                summary['Pure'] = (summary['Metal 18kt'] * 0.76).apply(std_round)
                
                m1, m2, m3 = st.columns(3)
                m1.metric(f"Total Bags ({title})", summary['Bag_No'].sum())
                m2.metric(f"18kt Required", f"{summary['Metal 18kt'].sum()}g")
                m3.metric(f"Pure Gold (76%)", f"{summary['Pure'].sum()}g")
                
                st.table(summary[['Bag_No', 'Metal 18kt', 'Pure']].rename(columns={'Bag_No': 'Qty'}))

            with st.expander("👤 CUSTOMER ORDERS", expanded=True):
                c_df = pending_df[pending_df['Order_Type'].str.contains('CUSTOMER', case=False, na=False)]
                if not c_df.empty: create_exec_summary(c_df, "Customer")
            with st.expander("📦 STOCK ORDERS", expanded=True):
                s_df = pending_df[pending_df['Order_Type'].str.contains('STOCK', case=False, na=False)]
                if not s_df.empty: create_exec_summary(s_df, "Stock")

        elif report_choice == "📋 CSR (Status Report)":
            st.header("📋 CSR: Customer Status Report")
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
                    st.write(f"**Total:** {summary['Bag_No'].sum()} Bags | {summary['Metal 18kt'].sum()}g Metal")

        else:
            st.header("🔍 Global Inventory Search")
            search = st.text_input("Enter Style No or Bag No...")
            if search:
                res = df[df['Style_No'].astype(str).str.contains(search, case=False) | df['Bag_No'].astype(str).str.contains(search, case=False)]
                st.dataframe(res, column_config={"Thumbnail_Link": st.column_config.ImageColumn("Preview"), "CAD_Link": st.column_config.LinkColumn("CAD")}, use_container_width=True, hide_index=True)
            else:
                st.dataframe(df.head(20), column_config={"Thumbnail_Link": st.column_config.ImageColumn("Preview"), "CAD_Link": st.column_config.LinkColumn("CAD")}, use_container_width=True, hide_index=True)
