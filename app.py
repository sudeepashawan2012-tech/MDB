import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime

# 1. INITIAL SETUP
st.set_page_config(page_title="WORKSHOP REPORTS", layout="wide")

scopes = ["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/drive"]
creds = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
client = bigquery.Client(credentials=creds, project=creds.project_id)

# 2. HELPER FUNCTIONS
def refresh_native_tables():
    try:
        queries = [
            """CREATE OR REPLACE TABLE `jewelry-sql-system.workshop_data.master_inventory_native` AS SELECT * FROM `jewelry-sql-system.workshop_data.master_inventory`""",
            """CREATE OR REPLACE TABLE `jewelry-sql-system.workshop_data.SALE_DATA_native` AS SELECT * FROM `jewelry-sql-system.workshop_data.SALE_DATA`""",
            """CREATE OR REPLACE TABLE `jewelry-sql-system.workshop_data.pre_finish_movement_native` CLUSTER BY BAG_NO AS SELECT * FROM `jewelry-sql-system.workshop_data.pre_finish_movement`""",
            """CREATE OR REPLACE TABLE `jewelry-sql-system.workshop_data.post_finish_movement_native` CLUSTER BY BAG_NO AS SELECT * FROM `jewelry-sql-system.workshop_data.post_finish_movement`"""
        ]
        for q in queries:
            client.query(q).result()
        st.sidebar.success("All Workshop Data Refreshed!")
        st.cache_data.clear() 
    except Exception as e:
        st.sidebar.error(f"Refresh Failed: {e}")

@st.cache_data(ttl=300)
def fetch_data():
    try:
        query = "SELECT * FROM `jewelry-sql-system.workshop_data.master_inventory_native`"
        df = client.query(query).to_dataframe()
        df.columns = [str(c).strip().upper().replace(' ', '_').replace('.', '_').replace('/', '_') for c in df.columns]
        return df
    except Exception as e:
        st.error(f"Connection Error: {e}")
        return None

@st.cache_data(ttl=300)
def fetch_sales_data():
    try:
        query = "SELECT * FROM `jewelry-sql-system.workshop_data.SALE_DATA_native`"
        df = client.query(query).to_dataframe()
        df.columns = [str(c).strip().upper().replace(' ', '_').replace('.', '_') for c in df.columns]
        return df
    except Exception as e:
        st.error(f"Sales Data Error: {e}")
        return None

def std_round(x):
    try: return int(float(x) + 0.5) if float(x) > 0 else 0
    except: return 0

def clean_date(dt):
    try:
        if pd.isna(dt) or str(dt).strip() == "" or str(dt) == "None": return "---"
        if isinstance(dt, str): dt = pd.to_datetime(dt)
        return dt.strftime('%d-%b-%Y')
    except: return str(dt)

# 3. RUN APP
if "password_correct" not in st.session_state:
    st.title("🔒 Login")
    pwd = st.text_input("Password", type="password")
    if st.button("Login"):
        if pwd == st.secrets["workshop_password"]:
            st.session_state["password_correct"] = True
            st.rerun()
else:
    df = fetch_data()
    df_sales = fetch_sales_data()

    if df is not None:
        # Standard Column Mappings
        col_bag = next((c for c in df.columns if 'BAG' in c), 'BAG_NO')
        col_cust = next((c for c in df.columns if 'CUSTOMER' in c), 'CUSTOMER')
        col_dia = next((c for c in df.columns if 'DIA' in c and 'CTS' in c), 'DIA_CTS')
        col_metal = next((c for c in df.columns if 'METAL' in c and '18' in c and 'WT' in c), 'METAL_18KT_WT')
        col_status = next((c for c in df.columns if 'STATUS' in c and 'DATE' not in c), 'CURRENT_STATUS')
        col_order_type = next((c for c in df.columns if 'ORDER_TYPE' in c), 'ORDER_TYPE')
        col_issue_dt = next((c for c in df.columns if 'METAL' in c and 'ISSUE' in c and 'DATE' in c), 'METAL_ISSUE_DATE')

        df[col_metal] = pd.to_numeric(df[col_metal], errors='coerce').fillna(0)
        df[col_dia] = pd.to_numeric(df[col_dia], errors='coerce').fillna(0)

        menu = st.sidebar.radio("SELECT REPORT", ["📊 Metal Requirements", "📋 CSR", "📋 Scope of Work", "🔍 Bag History Report", "📈 Sales Analytics"])
        st.sidebar.divider()
        if st.sidebar.button("🔄 REFRESH MOVEMENT DATA"):
            with st.spinner("Syncing..."): refresh_native_tables()

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
                else: st.info(f"No Pending {o_type} Orders")

        # --- REPORT 2: CSR ---
        elif menu == "📋 CSR":
            st.header("📋 Customer Status Report")
            status_seq = {"SEQUENCE": 0, "ENGRAVING/HUID": 1, "IGI": 2, "ON HAND": 3, "FINAL QC": 4, "SETTING QC OK": 5, "SETTING": 6, "GHAT OK": 7, "CASTING": 8, "METAL ISSUED": 9, "METAL PENDING": 10, "HOLD": 12, "CANCEL": 13}
            csr_df = df.copy()
            csr_df['Seq'] = csr_df[col_status].map(status_seq).fillna(99)
            # FIX: Convert to string before sorting to prevent TypeError
            customers = sorted([str(x) for x in csr_df[col_cust].unique() if pd.notna(x)])
            for cust in customers:
                with st.expander(f"👤 CUSTOMER: {cust}"):
                    cust_data = csr_df[csr_df[col_cust] == cust]
                    summary = cust_data.groupby([col_status, 'Seq']).agg({col_bag: 'count', col_metal: 'sum', col_dia: 'sum'}).reset_index().sort_values('Seq')
                    summary['Metal 18kt'] = summary[col_metal].apply(std_round)
                    summary['Dia Cts'] = summary[col_dia].map('{:,.2f}'.format)
                    st.dataframe(summary[[col_status, col_bag, 'Metal 18kt', 'Dia Cts']].rename(columns={col_status: 'Status', col_bag: 'Bag Qty'}), hide_index=True, use_container_width=True)

        # --- REPORT 3: BAG HISTORY ---
        elif menu == "🔍 Bag History Report":
            st.header("🔍 Bag History Report")
            search_bag = st.text_input("Enter Bag Number to Search").strip()
            if search_bag:
                match = df[df[col_bag].astype(str).str.upper() == search_bag.upper()]
                if not match.empty:
                    r = match.iloc[0]
                    c1, c2 = st.columns([2, 1])
                    with c1:
                        st.markdown("### 📦 Bag Master Details")
                        sub1, sub2 = st.columns(2)
                        with sub1:
                            st.write(f"**Customer:** {r.get(col_cust, 'N/A')}"); st.write(f"**Type:** {r.get(col_order_type, 'N/A')}")
                            st.write(f"**Metal:** {std_round(r.get(col_metal, 0))}g 18kt"); st.write(f"**Dia:** {float(r.get(col_dia, 0)):.2f} Cts")
                        with sub2:
                            st.write(f"**Ordered:** {clean_date(r.get('ORDER_DATE'))}"); st.write(f"**Status:** {r.get(col_status, 'N/A')}")
                    with col_img:
                        img_url = r.get('IMAGE_LINK')
                        if img_url and str(img_url).strip() not in ["", "---", "None"]:
                            if "id=" in str(img_url): file_id = str(img_url).split("id=")[1].split("&")[0]
                            elif "d/" in str(img_url): file_id = str(img_url).split("d/")[1].split("/")[0]
                            else: file_id = None
                            if file_id: st.image(f"https://drive.google.com/uc?export=view&id={file_id}", width=250)
                    
                    st.divider()
                    st.markdown("### 📋 QC Process Report")
                    def get_smart_val(letter, default="---"):
                        potential_names = [letter, f"_{letter}_", f"COLUMN_{letter}", letter.upper()]
                        col = next((name for name in potential_names if name in match.columns), None)
                        return r[col] if col and pd.notna(r[col]) else default

                    q1, q2, q3 = st.columns(3)
                    with q1:
                        st.markdown("**🛠️ GHAT DETAILS**"); st.write(f"QC: {get_smart_val('X')}"); st.write(f"Weight: {get_smart_val('Y', '0')}g")
                    with q2:
                        st.markdown("**💎 SETTING DETAILS**"); st.write(f"QC: {get_smart_val('AH')}"); st.write(f"Weight: {get_smart_val('AY', '0')}g")
                    with q3:
                        st.markdown("**✨ FINAL FINISH**"); st.write(f"Final QC: {get_smart_val('AK')}"); st.write(f"Final Wt: {get_smart_val('AL', '0')}g")

                    st.divider()
                    st.subheader("🛠️ MOVEMENT LOGS")
                    def get_mov(t_id):
                        m = client.query(f"SELECT * FROM `jewelry-sql-system.workshop_data.{t_id}` WHERE CAST(BAG_NO AS STRING) = '{search_bag}'").to_dataframe()
                        m.columns = [str(c).upper().replace(' ', '_') for c in m.columns]
                        return m
                    st.write("**Pre-Finish**"); st.dataframe(get_mov("pre_finish_movement_native"), hide_index=True)
                    st.write("**Post-Finish**"); st.dataframe(get_mov("post_finish_movement_native"), hide_index=True)
                else: st.warning("Bag not found.")

        # --- REPORT 4: SALES ANALYTICS ---
        elif menu == "📈 Sales Analytics":
            st.header("📈 Sales Analytics")
            if df_sales is not None and not df_sales.empty:
                s_cust = next((c for c in df_sales.columns if 'CUSTOMER' in c), None)
                s_dia = next((c for c in df_sales.columns if 'DIA' in c and 'CTS' in c), None)
                s_date = next((c for c in df_sales.columns if 'DATE' in c), None)
                if s_cust and s_dia and s_date:
                    df_sales[s_date] = pd.to_datetime(df_sales[s_date], errors='coerce')
                    df_sales = df_sales.dropna(subset=[s_date])
                    df_sales['Month'] = df_sales[s_date].dt.strftime('%b %Y')
                    df_sales[s_dia] = pd.to_numeric(df_sales[s_dia], errors='coerce').fillna(0)
                    df_sales['Type'] = df_sales[s_dia].apply(lambda x: 'Big Work (>5ct)' if x > 5 else 'Small Work (<=5ct)')
                    report = df_sales.groupby([s_cust, 'Month', 'Type'])[s_dia].sum().unstack(fill_value=0).reset_index()
                    for c in sorted(report[s_cust].unique()):
                        with st.expander(f"👤 {c}"): st.dataframe(report[report[s_cust] == c], hide_index=True)
                else: st.error("Required columns not found in Sales sheet.")
