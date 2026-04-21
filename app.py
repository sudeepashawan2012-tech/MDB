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
        sdf = client.query(query).to_dataframe()
        return sdf
    except Exception as e:
        st.error(f"Sales Data Fetch Error: {e}")
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

        menu = st.sidebar.radio("SELECT REPORT", ["📊 Metal Requirements", "📋 CSR", "📋 Scope of Work", "🔍 Bag History Report", "💰 Sales Report"])

        st.sidebar.divider()
        if st.sidebar.button("🔄 REFRESH MOVEMENT DATA"):
            with st.sidebar.spinner("Syncing..."):
                refresh_native_tables()

        # --- REPORT 1: METAL REQUIREMENTS ---
        if menu == "📊 Metal Requirements":
            st.header("📊 Metal Requirement Report")
            mask = (df[col_issue_dt].isna() | (df[col_issue_dt].astype(str).str.strip() == "")) & (~df[col_status].isin(["HOLD", "CANCEL"]))
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
                    st.markdown(f"**SUBTOTAL:** {sub_data[col_bag].count()} Bags | {std_round(sub_data[col_metal].sum())}g 18kt | {sub_data[col_dia].sum():,.2f} Dia Cts")
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
                    total_row = pd.DataFrame([{col_status: 'TOTAL', col_bag: summary[col_bag].sum(), col_metal: summary[col_metal].sum(), col_dia: summary[col_dia].sum()}])
                    final_summary = pd.concat([summary, total_row], ignore_index=True)
                    final_summary['Metal 18kt'] = final_summary[col_metal].apply(std_round)
                    final_summary['Dia Cts'] = final_summary[col_dia].map('{:,.2f}'.format)
                    st.dataframe(final_summary[[col_status, col_bag, 'Metal 18kt', 'Dia Cts']].rename(columns={col_status: 'Status', col_bag: 'Bag Qty'}), hide_index=True, use_container_width=True)

        # --- REPORT: SCOPE OF WORK ---
        elif menu == "📋 Scope of Work":
            st.header("📋 Scope of Work")
            def get_report_table(data):
                if data.empty: return None
                grp = data.groupby(col_cust).agg({col_bag: 'count', col_metal: 'sum', col_dia: 'sum'}).reset_index()
                grp.columns = ['Customer Name', 'Ord Qty', 'Metal 18kt', 'Dia Cts']
                total_row = pd.DataFrame([{'Customer Name': 'TOTAL', 'Ord Qty': grp['Ord Qty'].sum(), 'Metal 18kt': grp['Metal 18kt'].sum(), 'Dia Cts': grp['Dia Cts'].sum()}])
                final_df = pd.concat([grp, total_row], ignore_index=True)
                final_df['Metal 18kt'] = final_df['Metal 18kt'].apply(std_round)
                final_df['Dia Cts'] = final_df['Dia Cts'].map('{:,.2f}'.format)
                return final_df

            st.markdown(f"""<div style="background-color:#1E1E1E; padding:25px; border-radius:10px; border:2px solid #4F4F4F; text-align:center; color: white;">
                <div style="font-size:28px; font-weight:bold;">{df[col_bag].count()} Ord Qty | {std_round(df[col_metal].sum())} Metal 18kt | {df[col_dia].sum():,.2f} Dia Cts</div></div>""", unsafe_allow_html=True)
            
            issued_mask = df[col_issue_dt].notna() & (df[col_issue_dt].astype(str).str.strip() != "")
            is_cust = df[col_order_type].str.contains("CUSTOMER", case=False, na=False)
            is_stock = df[col_order_type].str.contains("STOCK", case=False, na=False)
            
            for title, mask_data in [("Customer Orders", df[is_cust]), ("Stock Orders", df[is_stock]), ("Metal Issued Customer", df[issued_mask & is_cust]), ("Metal Pending Customer", df[~issued_mask & is_cust])]:
                st.markdown(f"### {title}")
                tbl = get_report_table(mask_data)
                if tbl is not None: st.table(tbl)
                else: st.info("No data")
                st.divider()

        # --- REPORT 3: BAG HISTORY ---
        elif menu == "🔍 Bag History Report":
            st.header("🔍 Bag History Report")
            search_bag = st.text_input("Enter Bag Number to Search").strip()
            if search_bag:
                match = df[df[col_bag].astype(str).str.upper() == search_bag.upper()]
                sdf = fetch_sales_data()
                sale_record = None
                if sdf is not None:
                    sale_match = sdf[sdf.iloc[:, 5].astype(str).str.upper() == search_bag.upper()]
                    if not sale_match.empty: sale_record = sale_match.iloc[0]

                if not match.empty or sale_record is not None:
                    r = match.iloc[0] if not match.empty else None
                    is_sold = sale_record is not None
                    
                    d_status = "SOLD" if is_sold else (r.get(col_status, 'N/A') if r is not None else "OUT OF STOCK")
                    d_cust = sale_record.iloc[0] if is_sold else (r.get(col_cust, 'N/A') if r is not None else "N/A")
                    d_deliv = clean_date(sale_record.iloc[19]) if is_sold else (clean_date(r.get('DELIVERY_DATE')) if r is not None else "---")

                    c_det, c_img = st.columns([2, 1])
                    with c_det:
                        st.markdown("### 📦 Bag Master Details")
                        if is_sold: st.error("🚨 ITEM SOLD")
                        s1, s2 = st.columns(2)
                        with s1:
                            st.write(f"**Customer:** {d_cust}")
                            st.write(f"**Type:** {r.get(col_order_type, 'N/A') if r is not None else 'N/A'}")
                            st.write(f"**Karigar:** {r.get('KARIGAR', 'N/A') if r is not None else 'N/A'}")
                            v_metal = sale_record.iloc[10] if is_sold else (r.get(col_metal, 0) if r is not None else 0)
                            v_dia = sale_record.iloc[11] if is_sold else (r.get(col_dia, 0) if r is not None else 0)
                            st.write(f"**Metal:** {std_round(v_metal)}g 18kt")
                            st.write(f"**Dia:** {float(v_dia):.2f} Cts")
                        with s2:
                            st.write(f"**Ordered:** {clean_date(r.get('ORDER_DATE')) if r is not None else '---'}")
                            st.write(f"**Metal Iss:** {clean_date(r.get(col_issue_dt)) if r is not None else '---'}")
                            st.write(f"**Deliv Dt:** {d_deliv}")
                            st.write(f"**Status:** {d_status}")
                            if is_sold: st.caption(f"✨ Sales Bag: {sale_record.iloc[5]}")

                    with c_img:
                        st.markdown("### 🖼️ Design")
                        img_url = r.get('IMAGE_LINK') if r is not None else None
                        if img_url and str(img_url).strip() not in ["", "---", "None"]:
                            if "id=" in str(img_url): fid = str(img_url).split("id=")[1].split("&")[0]
                            elif "d/" in str(img_url): fid = str(img_url).split("d/")[1].split("/")[0]
                            else: fid = None
                            if fid: st.markdown(f'<a href="{img_url}" target="_blank"><img src="https://lh3.googleusercontent.com/u/0/d/{fid}" width="100%" style="border-radius:10px; border:1px solid #4F4F4F;"></a>', unsafe_allow_html=True)
                        else: st.info("No Image")

                    st.divider()
                    if not match.empty:
                        st.markdown("### 📋 QC Process Report")
                        def find_col(l):
                            for n in [l, f"_{l}_", f"COLUMN_{l}", l.upper()]:
                                if n in match.columns: return n
                            return None
                        def get_v(l):
                            c = find_col(l)
                            return r[c] if c and pd.notna(r[c]) else "---"
                        q1, q2, q3 = st.columns(3)
                        with q1: st.write(f"**🛠️ GHAT:** QC {get_v('X')} | {get_v('Y')}g")
                        with q2: st.write(f"**💎 SETTING:** QC {get_v('AH')} | {get_v('AY')}g")
                        with q3: st.write(f"**✨ FINAL:** QC {get_v('AK')} | {get_v('AL')}g")
                        
                        try:
                            def get_mv(tid):
                                q = f"SELECT * FROM `jewelry-sql-system.workshop_data.{tid}` WHERE CAST(BAG_NO AS STRING) = '{search_bag}'"
                                mdf = client.query(q).to_dataframe()
                                if not mdf.empty:
                                    mdf.columns = [str(c).upper().replace(' ', '_') for c in mdf.columns]
                                return mdf
                            st.markdown("### 🛠️ MOVEMENT DATA")
                            pre, post = get_mv("pre_finish_movement_native"), get_mv("post_finish_movement_native")
                            if not pre.empty: st.caption("Pre-Finish"); st.dataframe(pre, hide_index=True)
                            if not post.empty: st.caption("Post-Finish"); st.dataframe(post, hide_index=True)
                        except Exception as me: st.error(f"Movement Error: {me}")
                else:
                    st.warning(f"Bag No {search_bag} not found.")

        # --- REPORT 4: SALES REPORT ---
        elif menu == "💰 Sales Report":
            st.header("💰 Month-wise Sales Report")
            sdf = fetch_sales_data()
            if sdf is not None:
                try:
                    proc_df = pd.DataFrame({'Customer': sdf.iloc[:,0], 'Metal': pd.to_numeric(sdf.iloc[:,10], errors='coerce').fillna(0), 'Dia': pd.to_numeric(sdf.iloc[:,11], errors='coerce').fillna(0), 'Date': pd.to_datetime(sdf.iloc[:,19], errors='coerce')}).dropna(subset=['Date'])
                    proc_df['Month_Year'] = proc_df['Date'].dt.strftime('%b-%y')
                    for month in proc_df.sort_values('Date')['Month_Year'].unique():
                        st.subheader(f"📅 {month}")
                        m_data = proc_df[proc_df['Month_Year'] == month]
                        summary = m_data.groupby('Customer').agg({'Metal': 'sum', 'Dia': 'sum'}).reset_index()
                        total_row = pd.DataFrame([{'Customer': 'TOTAL', 'Metal': summary['Metal'].sum(), 'Dia': summary['Dia'].sum()}])
                        final = pd.concat([summary, total_row], ignore_index=True)
                        final['Metal 18kt'] = final['Metal'].apply(std_round)
                        final['Dia cts'] = final['Dia'].map('{:,.2f}'.format)
                        st.table(final[['Customer', 'Metal 18kt', 'Dia cts']])
                except Exception as e: st.error(f"Sales Report Error: {e}")
