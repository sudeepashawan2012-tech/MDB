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
        
        query = "SELECT * FROM `jewelry-sql-system.workshop_data.master_inventory`"
        df = client.query(query).to_dataframe()
        
        # Clean column names: Uppercase and replace spaces/dots with underscores
        df.columns = [str(c).strip().upper().replace(' ', '_').replace('.', '_').replace('/', '_') for c in df.columns]
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
        # --- DYNAMIC COLUMN MAPPING ---
        # This finds the column even if it's named 'METAL_18KT_WT' or 'METAL_18KT' or 'METAL_18_KT_WT'
        col_metal = next((c for c in df.columns if 'METAL' in c and '18' in c and 'WT' in c), None)
        col_status = next((c for c in df.columns if 'STATUS' in c and 'DATE' not in c), None)
        col_cust = next((c for c in df.columns if 'CUSTOMER' in c), 'CUSTOMER')
        col_order_type = next((c for c in df.columns if 'ORDER_TYPE' in c or 'ORDER_TYPE' in c), 'ORDER_TYPE')
        col_bag = next((c for c in df.columns if 'BAG' in c), 'BAG_NO')
        col_dia = next((c for c in df.columns if 'DIA' in c and 'CTS' in c), 'DIA_CTS')
        col_issue_dt = next((c for c in df.columns if 'METAL' in c and 'ISSUE' in c and 'DATE' in c), None)

        # Standardize Numeric Data using the mapped names
        if col_metal:
            df[col_metal] = pd.to_numeric(df[col_metal], errors='coerce').fillna(0)
        if col_dia:
            df[col_dia] = pd.to_numeric(df[col_dia], errors='coerce').fillna(0)

        menu = st.sidebar.radio("SELECT REPORT", ["📊 Metal Requirements", "📋 CSR"])

        # --- REPORT 1: METAL REQUIREMENTS ---
        if menu == "📊 Metal Requirements":
            st.header("📊 Metal Requirement Report")
            
            if not col_issue_dt or not col_status:
                st.error("Could not find Status or Metal Issue Date columns in Database.")
            else:
                exclude = ["HOLD", "CANCEL"]
                mask = (df[col_issue_dt].isna() | (df[col_issue_dt].astype(str).str.strip() == "")) & (~df[col_status].isin(exclude))
                pending_df = df[mask].copy()

                for o_type in ["CUSTOMER", "STOCK"]:
                    st.subheader(f"📍 {o_type} ORDERS")
                    sub_data = pending_df[pending_df[col_order_type].str.contains(o_type, case=False, na=False)]
                    
                    if not sub_data.empty:
                        summary = sub_data.groupby(col_cust).agg({
                            col_bag: 'count',
                            col_metal: 'sum',
                            col_dia: 'sum'
                        }).reset_index()
                        
                        summary.columns = ['Customer Code', 'Bag Qty', 'Metal 18kt', 'Dia Cts']
                        summary['Metal 18kt'] = summary['Metal 18kt'].apply(std_round)
                        st.table(summary)
                        
                        t_bags = sub_data[col_bag].count()
                        t_metal = std_round(sub_data[col_metal].sum())
                        st.markdown(f"**SUBTOTAL:** `{t_bags}` Bags | `{t_metal}g` 18kt")
                    else:
                        st.write(f"No pending {o_type} requirements.")

        # --- REPORT 2: CSR ---
        elif menu == "📋 CSR":
            st.header("📋 Customer Status Report")
            
            if not col_status:
                st.error("Could not find Status column in Database.")
            else:
                status_seq = {
                    "SEQUENCE": 0, "ENGRAVING/HUID": 1, "IGI": 2, "ON HAND": 3, 
                    "FINAL QC": 4, "SETTING QC OK": 5, "SETTING": 6, "GHAT OK": 7, 
                    "CASTING": 8, "METAL ISSUED": 9, "METAL PENDING": 10, 
                    "HOLD": 12, "CANCEL": 13
                }
                
                csr_df = df.copy()
                csr_df['Seq'] = csr_df[col_status].map(status_seq).fillna(99)

                customers = sorted(csr_df[col_cust].unique())
                for cust in customers:
                    with st.expander(f"👤 CUSTOMER: {cust}"):
                        cust_data = csr_df[csr_df[col_cust] == cust]
                        summary = cust_data.groupby([col_status, 'Seq']).agg({
                            col_bag: 'count',
                            col_metal: 'sum',
                            col_dia: 'sum'
                        }).reset_index().sort_values('Seq')
                        
                        summary['Metal 18kt'] = summary[col_metal].apply(std_round)
                        
                        display_tab = summary[[col_status, col_bag, 'Metal 18kt', col_dia]].rename(
                            columns={col_status: 'Status', col_bag: 'Bag Qty', col_dia: 'Dia Cts'}
                        )
                        st.table(display_tab)
                        st.markdown(f"**TOTAL:** `{summary[col_bag].sum()}` Bags | `{std_round(summary[col_metal].sum())}g` 18kt")
