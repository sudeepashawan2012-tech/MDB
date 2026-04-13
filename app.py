import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# 1. SETUP
st.set_page_config(page_title="WORKSHOP REPORTS", layout="wide")

@st.cache_data(ttl=300)
def fetch_data():
    try:
        scopes = ["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/drive"]
        creds = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
        client = bigquery.Client(credentials=creds, project=creds.project_id)
        
        # Exact SQL Query
        query = "SELECT * FROM `jewelry-sql-system.workshop_data.master_inventory`"
        df = client.query(query).to_dataframe()
        
        # CLEANING: Remove trailing spaces from column names to prevent KeyErrors
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        st.error(f"Connection Error: {e}")
        return None

def std_round(x):
    try: return int(float(x) + 0.5) if float(x) > 0 else 0
    except: return 0

# 2. RUN APP
if "password_correct" not in st.session_state:
    st.title("🔒 Login")
    pwd = st.text_input("Password", type="password")
    if st.button("Login"):
        if pwd == st.secrets["workshop_password"]:
            st.session_state["password_correct"] = True
            st.rerun()
else:
    df = fetch_data()

    if df is not None:
        # Standardize Numeric Columns immediately
        df['METAL 18KT WT'] = pd.to_numeric(df['METAL 18KT WT'], errors='coerce').fillna(0)
        df['DIA CTS'] = pd.to_numeric(df['DIA CTS'], errors='coerce').fillna(0)

        menu = st.sidebar.radio("SELECT REPORT", ["📊 Metal Requirements", "📋 CSR"])

        # --- REPORT 1: METAL REQUIREMENTS ---
        if menu == "📊 Metal Requirements":
            st.header("📊 Metal Requirement Report")
            st.info("Criteria: Metal Issue Date is Blank | Excludes: HOLD, CANCEL")
            
            # Logic
            exclude = ["HOLD", "CANCEL"]
            # Filter: Metal Issue Date is null/empty AND Status is not in exclude list
            mask = (df['METAL ISSUE DATE'].isna() | (df['METAL ISSUE DATE'] == "")) & (~df['CURRENT STATUS'].isin(exclude))
            pending_df = df[mask].copy()

            # Split into Customer and Stock
            for o_type in ["CUSTOMER ORDER", "STOCK ORDER"]:
                st.subheader(f"📍 {o_type}")
                sub_data = pending_df[pending_df['ORDER TYPE'].str.contains(o_type.split()[0], case=False, na=False)]
                
                if not sub_data.empty:
                    # Grouping
                    summary = sub_data.groupby('CUSTOMER').agg({
                        'BAG NO': 'count',
                        'METAL 18KT WT': 'sum',
                        'DIA CTS': 'sum'
                    }).reset_index()
                    
                    summary.columns = ['Customer Code', 'Bag Qty', 'Metal 18kt', 'Dia Cts']
                    summary['Metal 18kt'] = summary['Metal 18kt'].apply(std_round)
                    summary['Dia Cts'] = summary['Dia Cts'].map('{:,.3f}'.format)
                    
                    st.table(summary)
                    
                    # Totals
                    t_bags = sub_data['BAG NO'].count()
                    t_metal = std_round(sub_data['METAL 18KT WT'].sum())
                    st.markdown(f"**SUBTOTAL:** `{t_bags}` Bags | `{t_metal}g` 18kt")
                else:
                    st.write("No pending requirements for this category.")

        # --- REPORT 2: CSR (CUSTOMER STATUS REPORT) ---
        elif menu == "📋 CSR":
            st.header("📋 Customer Status Report")
            
            # Sorting Logic
            status_seq = {
                "SEQUENCE": 0, "ENGRAVING/HUID": 1, "IGI": 2, "ON HAND": 3, 
                "FINAL QC": 4, "SETTING QC OK": 5, "SETTING": 6, "GHAT OK": 7, 
                "CASTING": 8, "METAL ISSUED": 9, "METAL PENDING": 10, 
                "HOLD": 12, "CANCEL": 13
            }
            
            csr_df = df.copy()
            csr_df['Seq'] = csr_df['CURRENT STATUS'].map(status_seq).fillna(99)

            # Customer Expander Loop
            customers = sorted(csr_df['CUSTOMER'].unique())
            for cust in customers:
                with st.expander(f"👤 CUSTOMER: {cust}"):
                    cust_data = csr_df[csr_df['CUSTOMER'] == cust]
                    
                    # Group by Status and include the Seq for sorting
                    summary = cust_data.groupby(['CURRENT STATUS', 'Seq']).agg({
                        'BAG NO': 'count',
                        'METAL 18KT WT': 'sum',
                        'DIA CTS': 'sum'
                    }).reset_index().sort_values('Seq')
                    
                    # Formatting
                    summary['Metal 18kt'] = summary['METAL 18KT WT'].apply(std_round)
                    summary['Dia Cts'] = summary['DIA CTS'].map('{:,.3f}'.format)
                    
                    # Display Table
                    display_tab = summary[['CURRENT STATUS', 'BAG NO', 'Metal 18kt', 'Dia Cts']].rename(
                        columns={'CURRENT STATUS': 'Status', 'BAG NO': 'Bag Qty'}
                    )
                    st.table(display_tab)
                    
                    # Grand Total for Customer
                    st.markdown(f"**TOTAL:** `{summary['BAG NO'].sum()}` Bags | `{std_round(summary['METAL 18KT WT'].sum())}g` 18kt")
