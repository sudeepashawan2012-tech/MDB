import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime

# 1. INITIAL SETUP & CLIENT DEFINITION
st.set_page_config(page_title="WORKSHOP REPORTS", layout="wide")

scopes = ["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/drive"]
creds = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
client = bigquery.Client(credentials=creds, project=creds.project_id)

# 2. HELPER FUNCTIONS
def get_drive_direct_link(url):
    try:
        if "id=" in str(url):
            file_id = str(url).split("id=")[1].split("&")[0]
        elif "d/" in str(url):
            file_id = str(url).split("d/")[1].split("/")[0]
        else:
            return None
        return f"https://drive.google.com/uc?export=view&id={file_id}"
    except:
        return None

def refresh_native_tables():
    try:
        queries = [
            """CREATE OR REPLACE TABLE `jewelry-sql-system.workshop_data.master_inventory_native` 
               AS SELECT * FROM `jewelry-sql-system.workshop_data.master_inventory`""",
            """CREATE OR REPLACE TABLE `jewelry-sql-system.workshop_data.SALE_DATA_native` 
               AS SELECT * FROM `jewelry-sql-system.workshop_data.SALE_DATA`""",
            """CREATE OR REPLACE TABLE `jewelry-sql-system.workshop_data.pre_finish_movement_native` 
               CLUSTER BY BAG_NO AS SELECT * FROM `jewelry-sql-system.workshop_data.pre_finish_movement`""",
            """CREATE OR REPLACE TABLE `jewelry-sql-system.workshop_data.post_finish_movement_native` 
               CLUSTER BY BAG_NO AS SELECT * FROM `jewelry-sql-system.workshop_data.post_finish_movement`"""
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
        col_cust_check = next((c for c in df.columns if 'CUSTOMER' in c), None)
        if col_cust_check:
            df = df.dropna(subset=[col_cust_check])
            df = df[df[col_cust_check].astype(str).str.strip() != ""]
        return df
    except Exception as e:
        st.error(f"Connection Error: {e}")
        return None

@st.cache_data(ttl=300)
def fetch_sales_data():
    try:
        query = "SELECT * FROM `jewelry-sql-system.workshop_data.SALE_DATA_native`"
        df = client.query(query).to_dataframe()
        df.columns = [str(c).strip().upper().replace(' ', '_').replace('.', '_').replace('/', '_') for c in df.columns]
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

# 3. RUN APP (Login Logic)
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
        col_metal = next((c for c in df.columns if 'METAL' in c and '18' in c and 'WT' in c), 'METAL_18KT_WT')
        col_status = next((c for c in df.columns if 'STATUS' in c and 'DATE' not in c), 'CURRENT_STATUS')
        col_cust = next((c for c in df.columns if 'CUSTOMER' in c), 'CUSTOMER')
        col_order_type = next((c for c in df.columns if 'ORDER_TYPE' in c), 'ORDER_TYPE')
        col_bag = next((c for c in df.columns if 'BAG' in c), 'BAG_NO')
        col_dia = next((c for c in df.columns if 'DIA' in c and 'CTS' in c), 'DIA_CTS')
        col_issue_dt = next((c for c in df.columns if 'METAL' in c and 'ISSUE' in c and 'DATE' in c), 'METAL_ISSUE_DATE')

        df[col_metal] = pd.to_numeric(df[col_metal], errors='coerce').fillna(0)
        df[col_dia] = pd.to_numeric(df[col_dia], errors='coerce').fillna(0)

        menu = st.sidebar.radio("SELECT REPORT", ["📊 Metal Requirements", "📋 CSR", "📋 Scope of Work", "🔍 Bag History Report", "📈 Sales Analytics"])

        st.sidebar.divider()
        if st.sidebar.button("🔄 REFRESH MOVEMENT DATA"):
            with st.sidebar.spinner("Syncing..."):
                refresh_native_tables()

        # --- REPORT: SALES ANALYTICS ---
        if menu == "📈 Sales Analytics":
            st.header("📈 Sales Analytics (Customer & Month-wise)")
            
            if df_sales is not None and not df_sales.empty:
                # 1. Identify columns in sales table
                s_cust = next((c for c in df_sales.columns if 'CUSTOMER' in c), 'CUSTOMER')
                s_dia = next((c for c in df_sales.columns if 'DIA' in c and 'CTS' in c), 'DIA_CTS')
                s_metal = next((c for c in df_sales.columns if 'METAL' in c and 'WT' in c), 'METAL_WT')
                s_date = next((c for c in df_sales.columns if 'DATE' in c), 'DATE')

                # 2. Clean and Prep Data
                df_sales[s_date] = pd.to_datetime(df_sales[s_date], errors='coerce')
                df_sales = df_sales.dropna(subset=[s_date])
                df_sales['Month'] = df_sales[s_date].dt.strftime('%b %Y')
                df_sales[s_dia] = pd.to_numeric(df_sales[s_dia], errors='coerce').fillna(0)
                df_sales[s_metal] = pd.to_numeric(df_sales[s_metal], errors='coerce').fillna(0)

                # 3. Categorize Big vs Small (Big > 5 Cts)
                df_sales['Work Type'] = df_sales[s_dia].apply(lambda x: 'Big Work' if x > 5 else 'Small Work')

                # 4. Grouping
                sales_report = df_sales.groupby([s_cust, 'Month', 'Work Type']).agg({
                    s_dia: 'sum',
                    s_metal: 'sum'
                }).reset_index()

                # Pivot for a better view
                pivot_report = sales_report.pivot_table(
                    index=[s_cust, 'Month'], 
                    columns='Work Type', 
                    values=[s_dia, s_metal], 
                    fill_value=0
                ).reset_index()

                # Clean column names for the table
                pivot_report.columns = ['Customer', 'Month', 'Big Work (Cts)', 'Small Work (Cts)', 'Big Work (Metal)', 'Small Work (Metal)']

                # Display per customer
                for cust in sorted(pivot_report['Customer'].unique()):
                    with st.expander(f"👤 {cust} - Sales Breakdown"):
                        c_data = pivot_report[pivot_report['Customer'] == cust].copy()
                        c_data['Total Cts'] = c_data['Big Work (Cts)'] + c_data['Small Work (Cts)']
                        
                        # Formatting
                        c_data['Big Work (Cts)'] = c_data['Big Work (Cts)'].map('{:,.2f}'.format)
                        c_data['Small Work (Cts)'] = c_data['Small Work (Cts)'].map('{:,.2f}'.format)
                        c_data['Total Cts'] = c_data['Total Cts'].map('{:,.2f}'.format)
                        
                        st.dataframe(c_data[['Month', 'Big Work (Cts)', 'Small Work (Cts)', 'Total Cts']], hide_index=True, use_container_width=True)
            else:
                st.info("No data found in Sales Table. Please refresh or check the Google Sheet.")

        # --- EXISTING REPORTS (STAY UNCHANGED) ---
        elif menu == "📊 Metal Requirements":
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

        elif menu == "🔍 Bag History Report":
            st.header("🔍 Bag History Report")
            search_bag = st.text_input("Enter Bag Number to Search").strip()
            
            if search_bag:
                match = df[df[col_bag].astype(str).str.upper() == search_bag.upper()]
                if not match.empty:
                    r = match.iloc[0]
                    col_det, col_img = st.columns([2, 1])
                    with col_det:
                        st.markdown("### 📦 Bag Master Details")
                        sub1, sub2 = st.columns(2)
                        with sub1:
                            st.write(f"**Customer:** {r.get(col_cust, 'N/A')}")
                            st.write(f"**Type:** {r.get(col_order_type, 'N/A')}")
                            st.write(f"**Karigar:** {r.get('KARIGAR', 'N/A')}")
                            st.write(f"**Metal:** {std_round(r.get(col_metal, 0))}g 18kt")
                            st.write(f"**Dia:** {float(r.get(col_dia, 0)):.2f} Cts")
                        with sub2:
                            st.write(f"**Ordered:** {clean_date(r.get('ORDER_DATE'))}")
                            st.write(f"**Metal Iss:** {clean_date(r.get(col_issue_dt))}")
                            st.write(f"**Deliv Dt:** {clean_date(r.get('DELIVERY_DATE'))}")
                            st.write(f"**Status:** {r.get(col_status, 'N/A')}")
                    with col_img:
                        st.markdown("### 🖼️ Design")
                        img_url = r.get('IMAGE_LINK')
                        if img_url and str(img_url).strip() not in ["", "---", "None"]:
                            if "id=" in str(img_url): file_id = str(img_url).split("id=")[1].split("&")[0]
                            elif "d/" in str(img_url): file_id = str(img_url).split("d/")[1].split("/")[0]
                            else: file_id = None
                            if file_id:
                                thumb_url = f"https://lh3.googleusercontent.com/u/0/d/{file_id}"
                                st.markdown(f'<a href="{img_url}" target="_blank"><img src="{thumb_url}" width="100%" style="border-radius:10px; border:1px solid #4F4F4F;"></a>', unsafe_allow_html=True)
                                st.caption("👆 Click to enlarge")
                        else: st.info("No Image")
                    st.divider()
                    st.markdown("### 📋 QC Process Report")
                    # ... (rest of movement code remains exactly as you had it) ...
