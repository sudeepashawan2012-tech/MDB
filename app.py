import streamlit as st
import pandas as pd
import requests
import numpy as np

# 1. SETUP & CORPORATE STYLING
st.set_page_config(page_title="MASTER DATABASE | EXECUTIVE", layout="wide")

# Custom CSS for a professional look
st.markdown("""
    <style>
    .main { background-color: #f8f9fa; }
    .stMetric { background-color: #ffffff; padding: 15px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }
    div[data-testid="stExpander"] { border: none !important; box-shadow: 0 2px 4px rgba(0,0,0,0.05); background: white; border-radius: 10px; }
    </style>
    """, unsafe_allow_html=True) # FIXED THIS LINE

API_URL = "https://script.google.com/macros/s/AKfycbzJeiT_mTmPFVEFDqDZvnZeakdFVxUrGiOjtl-NBgGFHyi3HYLCO1648JSm7s2bW0A/exec"

@st.cache_data(ttl=60)
def fetch_data():
    try:
        response = requests.get(API_URL, timeout=15)
        raw_df = pd.DataFrame(response.json())
        
        # --- GHOST ROW CLEANER ---
        # Only keep rows that have a Bag Number AND a Customer Name
        if not raw_df.empty:
            raw_df = raw_df.replace('', np.nan) # Turn empty strings into real "None"
            raw_df = raw_df.dropna(subset=['Bag_No', 'Customer'], how='all')
            return raw_df
        return None
    except Exception as e:
        st.error(f"System Offline: {e}")
        return None

df = fetch_data()

def std_round(x):
    return int(x + 0.5) if x > 0 else 0

# --- SIDEBAR ---
with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/8002/8002150.png", width=100)
    st.title("MANAGEMENT HUB")
    st.divider()
    report_choice = st.radio("SELECT VIEW", ["🏠 Home / Search", "📊 Metal Requirements", "📋 CSR (Status Report)"])

# --- MAIN LOGIC ---
if df is not None:
    
    # --- REPORT 1: METAL REQUIREMENTS ---
    if report_choice == "📊 Metal Requirements":
        st.header("📊 Pending Metal Requirements")
        
        pending_mask = (df['Metal_Issue_Date'].isna()) | (df['Final_VZ_Status'] == "METAL PENDING")
        pending_df = df[pending_mask].copy()
        pending_df['Metal'] = pd.to_numeric(pending_df['Metal'], errors='coerce').fillna(0)

        def create_exec_summary(data, title, color):
            summary = data.groupby('Customer').agg({'Bag_No': 'count', 'Metal': 'sum'})
            summary['Metal 18kt'] = summary['Metal'].apply(std_round)
            summary['Pure'] = (summary['Metal 18kt'] * 0.76).apply(std_round)
            
            # Metric Row
            m1, m2, m3 = st.columns(3)
            m1.metric(f"Total Bags ({title})", summary['Bag_No'].sum())
            m2.metric(f"Total 18kt ({title})", f"{summary['Metal 18kt'].sum()}g")
            m3.metric(f"Total Pure ({title})", f"{summary['Pure'].sum()}g")
            
            display_df = summary[['Bag_No', 'Metal 18kt', 'Pure']].rename(columns={'Bag_No': 'Qty'})
            st.table(display_df)

        with st.expander("👤 CUSTOMER ORDERS", expanded=True):
            c_df = pending_df[pending_df['Order_Type'].str.contains('CUSTOMER', case=False, na=False)]
            create_exec_summary(c_df, "Customer", "blue") if not c_df.empty else st.write("Clear")

        with st.expander("📦 STOCK ORDERS", expanded=True):
            s_df = pending_df[pending_df['Order_Type'].str.contains('STOCK', case=False, na=False)]
            create_exec_summary(s_df, "Stock", "orange") if not s_df.empty else st.write("Clear")

    # --- REPORT 2: CSR ---
    elif report_choice == "📋 CSR (Status Report)":
        st.header("📋 Customer Status Report")
        
        status_seq = {
            "SEQUENCE": 0, "ENGRAVING/HUID": 1, "IGI": 2, "ON HAND": 3,
            "FINAL QC": 4, "SETTING QC OK": 5, "SETTING": 6, "GHAT OK": 7,
            "CASTING": 8, "METAL ISSUED": 9, "METAL PENDING": 10
        }

        csr_df = df.copy()
        csr_df['Metal'] = pd.to_numeric(csr_df['Metal'], errors='coerce').fillna(0)
        csr_df['Dia_Cts'] = pd.to_numeric(csr_df['Dia_Cts'], errors='coerce').fillna(0)
        csr_df['Seq'] = csr_df['Final_VZ_Status'].map(status_seq).fillna(99)

        for cust in sorted(csr_df['Customer'].unique()):
            with st.expander(f"👤 {cust}", expanded=False):
                cust_data = csr_df[csr_df['Customer'] == cust]
                summary = cust_data.groupby(['Final_VZ_Status', 'Seq']).agg({
                    'Bag_No': 'count', 'Metal': 'sum', 'Dia_Cts': 'sum'
                }).reset_index().sort_values('Seq')

                summary['Metal 18kt'] = summary['Metal'].apply(std_round)
                summary['Dia Cts'] = summary['Dia_Cts'].map('{:,.2f}'.format)
                
                res = summary[['Final_VZ_Status', 'Bag_No', 'Metal 18kt', 'Dia Cts']].rename(
                    columns={'Final_VZ_Status': 'Status', 'Bag_No': 'Qty'}
                )
                
                # Highlight Totals in the row
                st.table(res)
                st.write(f"**Customer Total:** {summary['Bag_No'].sum()} Bags | {summary['Metal 18kt'].sum()}g Metal | {summary['Dia_Cts'].sum():.2f} Cts")

    # --- HOME / SEARCH ---
    else:
        st.header("💎 Global Search")
        search = st.text_input("Enter Style No or Bag No...", placeholder="e.g. 10254")
        
        display_df = df.copy()
        if search:
            display_df = display_df[
                display_df['Style_No'].astype(str).str.contains(search, case=False, na=False) | 
                display_df['Bag_No'].astype(str).str.contains(search, case=False, na=False)
            ]

        st.dataframe(
            display_df,
            column_config={
                "Thumbnail_Link": st.column_config.ImageColumn("Preview"),
                "CAD_Link": st.column_config.LinkColumn("CAD")
            },
            use_container_width=True, hide_index=True
        )
