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
        # Dynamic Column Mapping
        col_metal = next((c for c in df.columns if 'METAL' in c and '18' in c and 'WT' in c), 'METAL_18KT_WT')
        col_status = next((c for c in df.columns if 'STATUS' in c and 'DATE' not in c), 'CURRENT_STATUS')
        col_cust = next((c for c in df.columns if 'CUSTOMER' in c), 'CUSTOMER')
        col_order_type = next((c for c in df.columns if 'ORDER_TYPE' in c), 'ORDER_TYPE')
        col_bag = next((c for c in df.columns if 'BAG' in c), 'BAG_NO')
        col_dia = next((c for c in df.columns if 'DIA' in c and 'CTS' in c), 'DIA_CTS')
        col_issue_dt = next((c for c in df.columns if 'METAL' in c and 'ISSUE' in c and 'DATE' in c), 'METAL_ISSUE_DATE')

        df[col_metal] = pd.to_numeric(df[col_metal], errors='coerce').fillna(0)
        df[col_dia] = pd.to_numeric(df[col_dia], errors='coerce').fillna(0)

        menu = st.sidebar.radio("SELECT REPORT", ["📊 Metal Requirements", "📋 CSR"])

        # --- REPORT 1: METAL REQUIREMENTS ---
        if menu == "📊 Metal Requirements":
            st.header("📊 Metal Requirement Report")
            exclude = ["HOLD", "CANCEL"]
            mask = (df[col_issue_dt].isna() | (df[col_issue_dt].astype(str).str.strip() == "")) & (~df[col_status].isin(exclude))
            pending_df = df[mask].copy()

            for o_type in ["CUSTOMER", "STOCK"]:
                st.subheader(f"📍 {o_type} ORDERS")
                sub_data = pending_df[pending_df[col_order_type].str.contains(o_type.split()[0], case=False, na=False)]
                
                if not sub_data.empty:
                    summary = sub_data.groupby(col_cust).agg({
                        col_bag: 'count',
                        col_metal: 'sum',
                        col_dia: 'sum'
                    }).reset_index()
                    
                    summary.columns = ['Customer Code', 'Bag Qty', 'Metal 18kt', 'Dia Cts']
                    summary['Metal 18kt'] = summary['Metal 18kt'].apply(std_round)
                    summary['Dia Cts'] = summary['Dia Cts'].map('{:,.2f}'.format)
                    
                    st.table(summary)
                    
                    # Totals with Large Bold Styling
                    t_bags = sub_data[col_bag].count()
                    t_metal = std_round(sub_data[col_metal].sum())
                    t_dia = sub_data[col_dia].sum()
                    st.markdown(f"""<div style="font-size:22px; font-weight:bold; border-top:2px solid #eee; padding-top:10px;">
                        SUBTOTAL: {t_bags} Bags | {t_metal}g 18kt | {t_dia:,.2f} Dia Cts
                        </div>""", unsafe_allow_html=True)
                else:
                    st.info(f"No Metal Pending For {o_type.title()} Orders")

        # --- REPORT 2: CSR ---
        elif menu == "📋 CSR":
            st.header("📋 Customer Status Report")
            status_seq = {
                "SEQUENCE": 0, "ENGRAVING/HUID": 1, "IGI": 2, "ON HAND": 3, 
                "FINAL QC": 4, "SETTING QC OK": 5, "SETTING": 6, "GHAT OK": 7, 
                "CASTING": 8, "METAL ISSUED": 9, "METAL PENDING": 10, 
                "HOLD": 12, "CANCEL": 13
            }
            
            csr_df = df.copy()
            csr_df[col_cust] = csr_df[col_cust].fillna("UNKNOWN")
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
                    summary['Dia Cts'] = summary[col_dia].map('{:,.2f}'.format)
                    
                    # Hide the index/Seq column by selecting only named columns
                    display_tab = summary[[col_status, col_bag, 'Metal 18kt', 'Dia Cts']].rename(
                        columns={col_status: 'Status', col_bag: 'Bag Qty'}
                    )
                    # hide_index=True ensures the red-marked column in Image 2 is gone
                    st.dataframe(display_tab, hide_index=True, use_container_width=True)
                    
                    t_cust_bags = summary[col_bag].sum()
                    t_cust_metal = std_round(summary[col_metal].sum())
                    t_cust_dia = summary[col_dia].sum()
                    st.markdown(f"""<div style="font-size:20px; font-weight:bold; border-top:1px solid #ccc; padding-top:5px; color:#1f77b4;">
                        TOTAL: {t_cust_bags} Bags | {t_cust_metal}g 18kt | {t_cust_dia:,.2f} Dia Cts
                        </div>""", unsafe_allow_html=True)
