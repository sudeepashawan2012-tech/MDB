import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime

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
        
        col_cust_check = next((c for c in df.columns if 'CUSTOMER' in c), None)
        if col_cust_check:
            df = df.dropna(subset=[col_cust_check])
            df = df[df[col_cust_check].astype(str).str.strip() != ""]
            
        return df
    except Exception as e:
        st.error(f"Connection Error: {e}")
        return None

def std_round(x):
    try: return int(float(x) + 0.5) if float(x) > 0 else 0
    except: return 0

def clean_date(dt):
    try:
        if pd.isna(dt) or str(dt).strip() == "" or str(dt) == "None": return "---"
        if isinstance(dt, str):
            dt = pd.to_datetime(dt)
        return dt.strftime('%d-%b-%Y')
    except:
        return str(dt)

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
        col_metal = next((c for c in df.columns if 'METAL' in c and '18' in c and 'WT' in c), 'METAL_18KT_WT')
        col_status = next((c for c in df.columns if 'STATUS' in c and 'DATE' not in c), 'CURRENT_STATUS')
        col_cust = next((c for c in df.columns if 'CUSTOMER' in c), 'CUSTOMER')
        col_order_type = next((c for c in df.columns if 'ORDER_TYPE' in c), 'ORDER_TYPE')
        col_bag = next((c for c in df.columns if 'BAG' in c), 'BAG_NO')
        col_dia = next((c for c in df.columns if 'DIA' in c and 'CTS' in c), 'DIA_CTS')
        col_issue_dt = next((c for c in df.columns if 'METAL' in c and 'ISSUE' in c and 'DATE' in c), 'METAL_ISSUE_DATE')

        df[col_metal] = pd.to_numeric(df[col_metal], errors='coerce').fillna(0)
        df[col_dia] = pd.to_numeric(df[col_dia], errors='coerce').fillna(0)

        # UPDATED SIDEBAR MENU
        menu = st.sidebar.radio("SELECT REPORT", ["📊 Metal Requirements", "📋 CSR", "📋 Scope of Work", "🔍 Bag History Report"])

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
                    summary = sub_data.groupby(col_cust).agg({col_bag: 'count', col_metal: 'sum', col_dia: 'sum'}).reset_index()
                    summary.columns = ['Customer Code', 'Bag Qty', 'Metal 18kt', 'Dia Cts']
                    summary['Metal 18kt'] = summary['Metal 18kt'].apply(std_round)
                    summary['Dia Cts'] = summary['Dia Cts'].map('{:,.2f}'.format)
                    st.table(summary)
                    
                    t_bags = sub_data[col_bag].count()
                    t_metal = std_round(sub_data[col_metal].sum())
                    t_dia = sub_data[col_dia].sum()
                    st.markdown(f"**SUBTOTAL:** {t_bags} Bags | {t_metal}g 18kt | {t_dia:,.2f} Dia Cts")
                else:
                    st.info(f"No Metal Pending For {o_type.title()} Orders")

        # --- REPORT 2: CSR ---
        elif menu == "📋 CSR":
            st.header("📋 Customer Status Report")
            status_seq = {"SEQUENCE": 0, "ENGRAVING/HUID": 1, "IGI": 2, "ON HAND": 3, "FINAL QC": 4, "SETTING QC OK": 5, "SETTING": 6, "GHAT OK": 7, "CASTING": 8, "METAL ISSUED": 9, "METAL PENDING": 10, "HOLD": 12, "CANCEL": 13}
            csr_df = df.copy()
            csr_df['Seq'] = csr_df[col_status].map(status_seq).fillna(99)
            for cust in sorted(csr_df[col_cust].unique()):
                with st.expander(f"👤 CUSTOMER: {cust}"):
                    cust_data = csr_df[csr_df[col_cust] == cust]
                    summary = cust_data.groupby([col_status, 'Seq']).agg({col_bag: 'count', col_metal: 'sum', col_dia: 'sum'}).reset_index().sort_values('Seq')
                    summary['Metal 18kt'] = summary[col_metal].apply(std_round)
                    summary['Dia Cts'] = summary[col_dia].map('{:,.2f}'.format)
                    st.dataframe(summary[[col_status, col_bag, 'Metal 18kt', 'Dia Cts']].rename(columns={col_status: 'Status', col_bag: 'Bag Qty'}), hide_index=True, use_container_width=True)

        # --- NEW REPORT: SCOPE OF WORK ---
        elif menu == "📋 Scope of Work":
            st.header("📋 Scope of Work")
            issued_mask = df[col_issue_dt].notna() & (df[col_issue_dt].astype(str).str.strip() != "")
            is_cust = df[col_order_type].str.contains("CUSTOMER", case=False, na=False)
            is_stock = df[col_order_type].str.contains("STOCK", case=False, na=False)
            
            def get_report_table(data):
                if data.empty: return None
                grp = data.groupby(col_cust).agg({col_bag: 'count', col_metal: 'sum', col_dia: 'sum'}).reset_index()
                grp.columns = ['Customer Name', 'Ord Qty', 'Metal 18kt', 'Dia Cts']
                total_row = pd.DataFrame([{'Customer Name': 'TOTAL', 'Ord Qty': grp['Ord Qty'].sum(), 'Metal 18kt': grp['Metal 18kt'].sum(), 'Dia Cts': grp['Dia Cts'].sum()}])
                final_df = pd.concat([grp, total_row], ignore_index=True)
                final_df['Metal 18kt'] = final_df['Metal 18kt'].apply(std_round)
                final_df['Dia Cts'] = final_df['Dia Cts'].map('{:,.2f}'.format)
                return final_df

            def display_section(title, data):
                st.markdown(f"### {title}")
                table = get_report_table(data)
                if table is not None: st.table(table)
                else: st.info(f"No data available for {title}")
                st.divider()

            gt_bags, gt_metal, gt_dia = df[col_bag].count(), std_round(df[col_metal].sum()), df[col_dia].sum()
            st.markdown(f"""<div style="background-color:#1E1E1E; padding:25px; border-radius:10px; border:2px solid #4F4F4F; text-align:center; color: white;">
                <div style="font-size:28px; font-weight:bold;">{gt_bags} Ord Qty | {gt_metal} Metal 18kt | {gt_dia:,.2f} Dia Cts</div></div>""", unsafe_allow_html=True)
            st.write("") 

            display_section("Customer Orders", df[is_cust])
            display_section("Stock Orders", df[is_stock])
            display_section("Metal Issued Customer Orders", df[issued_mask & is_cust])
            display_section("Metal Pending Customer Orders", df[~issued_mask & is_cust])
            display_section("Metal Issued Stock Orders", df[issued_mask & is_stock])
            display_section("Metal Pending Stock Orders", df[~issued_mask & is_stock])

        # --- REPORT 3: BAG HISTORY ---
        elif menu == "🔍 Bag History Report":
            st.header("🔍 Bag History Report")
            search_bag = st.text_input("Enter Bag Number to Search").strip()
            if search_bag:
                match = df[df[col_bag].astype(str).str.upper() == search_bag.upper()]
                if not match.empty:
                    r = match.iloc[0]
                    st.markdown("### 📦 Bag Master Details")
                    # ... (rest of your Bag History code remains unchanged)
                    st.write(f"**Customer:** {r.get(col_cust)}")
                    st.write(f"**Current Status:** {r.get(col_status)}")
