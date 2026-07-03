import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime

# ═══════════════════════════════════════════════════════════════
# COLUMN CONFIGURATION — Exact names from your BigQuery schema
# ═══════════════════════════════════════════════════════════════
COL_CUSTOMER = 'CUSTOMER'
COL_ORDER_DATE = 'ORDER_DATE'
COL_ORDER_TYPE = 'ORDER_TYPE'
COL_FORM = 'FORM_'                    # Form/Color column
COL_STATUS = 'CURRENT_STATUS'         # ✅ Actual status column
COL_BAG_NO = 'BAG_NO'
COL_STYLE_NO = 'STYLE_NO'
COL_PRODUCT_CODE = 'PRODUCT_CODE'     # Was ITEM before
COL_IMAGE_LINK = 'IMAGE_LINK'         # Main image column
COL_IMAGE = 'IMAGE'                   # Secondary image
COL_CAD = 'CAD'
COL_KARIGAR = 'KARIGAR'
COL_METAL_COLOUR = 'METAL_COLOUR'
COL_CUST_ORD_NO = 'CUST_ORD_NO'
COL_CUST_ORD_TYPE = 'CUST_ORD_TYPE'
COL_PRIORITY = 'PRIORITY'
COL_METAL_18KT = 'METAL_18KT_WT'      # ✅ Correct metal weight column
COL_DIA_CTS = 'DIA_CTS'
COL_METAL_ISSUE_DATE = 'METAL_ISSUE_DATE'
COL_DIA_ISSUE_DATE = 'DIA_ISSUE_DATE'
COL_DIA_2ND_ISSUE_DATE = 'DIA_2ND_ISSUE_DATE'
COL_KARIGAR_DOD = 'KARIGAR_DOD'
COL_DELIVERY_DATE = 'DELIVERY_DATE'
COL_IGI = 'IGI'
COL_GHAT_QC = 'GHAT_QC______'
COL_GHAT_WT = 'GHAT_WT______'
COL_GHAT_REMARK = 'REMARK______'
COL_GHAT_DATE = 'GHAT_DATE'
COL_CS_1ST_ISSUER = 'C_S_1ST_ISSUER'
COL_CS_1ST_QTY = 'C_S_1ST_ISSUE_QTY'
COL_CS_1ST_DATE = 'C_S_1ST_ISSUE_DATE'
COL_CS_2ND_ISSUER = 'C_S_2ND_ISSUER'
COL_CS_2ND_QTY = 'C_S_2ND_ISSUE_QTY'
COL_CS_2ND_DATE = 'C_S_2ND_ISSUE_DATE'
COL_SETTING_QC = 'SETTING_QC______'
COL_SETTING_WT = 'SETTING_WT______'
COL_SETTING_DATE = 'SETTING_DATE'
COL_FINAL_QC = 'FINAL_QC______'
COL_FINAL_WT = 'FINAL_WT______'
COL_FINAL_QC_REMARK = 'FINAL_QC_REMARK'
COL_FINAL_QC_DATE = 'FINAL_QC_DATE'
COL_FINISH = 'FINISH'
COL_FINISH_DATE = 'FINISH_DATE'
COL_IGI_SGL = 'IGI_SGL'
COL_IGI_DATE = 'IGI_DATE'
COL_HUID = 'HUID'
COL_HUID_DATE = 'HUID_DATE'
COL_STATUS_DATE = 'STATUS_DATE'

# ═══════════════════════════════════════════════════════════════
# 1. INITIAL SETUP & CLIENT DEFINITION
# ═══════════════════════════════════════════════════════════════
st.set_page_config(page_title="WORKSHOP REPORTS", layout="wide")

scopes = ["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/drive"]
creds = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
client = bigquery.Client(credentials=creds, project=creds.project_id)

# ═══════════════════════════════════════════════════════════════
# 2. HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════
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
        # Drop rows with blank customer
        if COL_CUSTOMER in df.columns:
            df = df.dropna(subset=[COL_CUSTOMER])
            df = df[df[COL_CUSTOMER].astype(str).str.strip() != ""]
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
        if isinstance(dt, str): dt = pd.to_datetime(dt, dayfirst=True)
        # Handle Excel serial numbers
        elif isinstance(dt, (int, float)) and dt > 40000 and dt < 60000:
            dt = pd.to_datetime(int(dt), unit='D', origin='1899-12-30')
        return dt.strftime('%d-%b-%Y')
    except: return str(dt)

def safe_get(row, col, default="---"):
    try:
        val = row.get(col)
        if pd.isna(val) or str(val).strip() in ["", "None", "nan"]: return default
        return val
    except:
        return default

def find_col(df, *candidates):
    """Find the first matching column from candidates."""
    for c in candidates:
        if c in df.columns:
            return c
    return None

# ═══════════════════════════════════════════════════════════════
# 3. RUN APP (Login Logic)
# ═══════════════════════════════════════════════════════════════
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
        # ─── Validate required columns exist ───
        required_cols = {
            'Customer': COL_CUSTOMER,
            'Order Type': COL_ORDER_TYPE,
            'Bag No': COL_BAG_NO,
            'Metal 18KT': COL_METAL_18KT,
            'Diamond Cts': COL_DIA_CTS,
            'Metal Issue Date': COL_METAL_ISSUE_DATE,
            'Status': COL_STATUS,
        }
        missing = [name for name, col in required_cols.items() if col not in df.columns]
        if missing:
            st.error("❌ **Missing Required Columns**")
            st.error(f"Not found: {', '.join(missing)}")
            st.write("**Available columns:**")
            st.code(", ".join(sorted(df.columns.tolist())))
            st.stop()

        # Convert numeric columns
        df[COL_METAL_18KT] = pd.to_numeric(df[COL_METAL_18KT], errors='coerce').fillna(0)
        df[COL_DIA_CTS] = pd.to_numeric(df[COL_DIA_CTS], errors='coerce').fillna(0)
        df[COL_GHAT_WT] = pd.to_numeric(df[COL_GHAT_WT], errors='coerce').fillna(0)
        df[COL_SETTING_WT] = pd.to_numeric(df[COL_SETTING_WT], errors='coerce').fillna(0)
        df[COL_FINAL_WT] = pd.to_numeric(df[COL_FINAL_WT], errors='coerce').fillna(0)

        # ─── SIDEBAR NAVIGATION ───
        st.sidebar.markdown("### 📊 MAIN REPORTS")
        menu = st.sidebar.radio("SELECT REPORT", [
            "📊 Metal Requirements",
            "📋 CSR",
            "📋 Scope of Work",
            "🔍 Bag History Report",
            "💰 Sales Analytics",
            "🔧 Diagnostics"
        ], label_visibility="collapsed")

        st.sidebar.divider()
        if st.sidebar.button("🔄 REFRESH MOVEMENT DATA"):
            with st.sidebar.spinner("Syncing..."):
                refresh_native_tables()

        # ═══════════════════════════════════════════════════════════════
        # 🔧 DIAGNOSTICS
        # ═══════════════════════════════════════════════════════════════
        if menu == "🔧 Diagnostics":
            st.header("🔧 Schema Diagnostics")
            
            st.subheader("📋 All Columns")
            st.code(", ".join(df.columns.tolist()), language="text")
            
            st.subheader("📊 Data Types")
            st.dataframe(df.dtypes.to_frame(name="Data_Type").reset_index().rename(columns={"index": "Column"}),
                        use_container_width=True, hide_index=True)
            
            st.subheader("🔍 First 3 Data Rows")
            st.dataframe(df.head(3), use_container_width=True)
            
            st.subheader(f"🔍 Unique Values in '{COL_STATUS}'")
            status_counts = df[COL_STATUS].value_counts().reset_index()
            status_counts.columns = [COL_STATUS, 'Count']
            st.dataframe(status_counts, use_container_width=True, hide_index=True)
            
            st.divider()
            st.subheader("📋 SALE_DATA_native")
            sdf = fetch_sales_data()
            if sdf is not None:
                st.code(", ".join(sdf.columns.tolist()), language="text")
                st.dataframe(sdf.head(3), use_container_width=True)
            else:
                st.error("Could not fetch sales data.")
            
            st.stop()

        # ═══════════════════════════════════════════════════════════════
        # 📊 METAL REQUIREMENTS
        # ═══════════════════════════════════════════════════════════════
        if menu == "📊 Metal Requirements":
            st.header("📊 Metal Requirement Report")
            exclude = ["HOLD", "CANCEL"]
            mask = (df[COL_METAL_ISSUE_DATE].isna() | (df[COL_METAL_ISSUE_DATE].astype(str).str.strip() == "")) & \
                   (~df[COL_STATUS].isin(exclude))
            pending_df = df[mask].copy()

            for o_type in ["CUSTOMER", "STOCK"]:
                st.subheader(f"📍 {o_type} ORDERS")
                sub_data = pending_df[pending_df[COL_ORDER_TYPE].str.contains(o_type.split()[0], case=False, na=False)]
                if not sub_data.empty:
                    summary = sub_data.groupby(COL_CUSTOMER).agg({
                        COL_BAG_NO: 'count',
                        COL_METAL_18KT: 'sum',
                        COL_DIA_CTS: 'sum'
                    }).reset_index()
                    summary.columns = ['Customer Code', 'Bag Qty', 'Metal 18kt', 'Dia Cts']
                    summary['Metal 18kt'] = summary['Metal 18kt'].apply(std_round)
                    summary['Dia Cts'] = summary['Dia Cts'].map('{:,.2f}'.format)
                    st.table(summary)
                    
                    t_bags = sub_data[COL_BAG_NO].count()
                    t_metal = std_round(sub_data[COL_METAL_18KT].sum())
                    t_dia = sub_data[COL_DIA_CTS].sum()
                    st.markdown(f"**SUBTOTAL:** {t_bags} Bags | {t_metal}g 18kt | {t_dia:,.2f} Dia Cts")
                else:
                    st.info(f"No Metal Pending For {o_type.title()} Orders")

        # ═══════════════════════════════════════════════════════════════
        # 📋 CSR
        # ═══════════════════════════════════════════════════════════════
        elif menu == "📋 CSR":
            st.header("📋 Customer Status Report")
            status_seq = {
                "SEQUENCE": 0, "ENGRAVING/HUID": 1, "IGI": 2, "ON HAND": 3,
                "FINAL QC": 4, "SETTING QC OK": 5, "SETTING": 6, "GHAT OK": 7,
                "CASTING": 8, "METAL ISSUED": 9, "METAL PENDING": 10,
                "HOLD": 12, "CANCEL": 13
            }
            csr_df = df.copy()
            csr_df['Seq'] = csr_df[COL_STATUS].map(status_seq).fillna(99)
            
            for cust in sorted(csr_df[COL_CUSTOMER].unique()):
                with st.expander(f"👤 CUSTOMER: {cust}"):
                    cust_data = csr_df[csr_df[COL_CUSTOMER] == cust]
                    summary = cust_data.groupby([COL_STATUS, 'Seq']).agg({
                        COL_BAG_NO: 'count',
                        COL_METAL_18KT: 'sum',
                        COL_DIA_CTS: 'sum'
                    }).reset_index().sort_values('Seq')
                    
                    total_row = pd.DataFrame([{
                        COL_STATUS: 'TOTAL',
                        COL_BAG_NO: summary[COL_BAG_NO].sum(),
                        COL_METAL_18KT: summary[COL_METAL_18KT].sum(),
                        COL_DIA_CTS: summary[COL_DIA_CTS].sum()
                    }])
                    
                    final_summary = pd.concat([summary, total_row], ignore_index=True)
                    final_summary['Metal 18kt'] = final_summary[COL_METAL_18KT].apply(std_round)
                    final_summary['Dia Cts'] = final_summary[COL_DIA_CTS].map('{:,.2f}'.format)
                    
                    st.dataframe(
                        final_summary[[COL_STATUS, COL_BAG_NO, 'Metal 18kt', 'Dia Cts']].rename(
                            columns={COL_STATUS: 'Status', COL_BAG_NO: 'Bag Qty'}),
                        hide_index=True, use_container_width=True
                    )

        # ═══════════════════════════════════════════════════════════════
        # 📋 SCOPE OF WORK
        # ═══════════════════════════════════════════════════════════════
        elif menu == "📋 Scope of Work":
            st.header("📋 Scope of Work")
            issued_mask = df[COL_METAL_ISSUE_DATE].notna() & (df[COL_METAL_ISSUE_DATE].astype(str).str.strip() != "")
            is_cust = df[COL_ORDER_TYPE].str.contains("CUSTOMER", case=False, na=False)
            is_stock = df[COL_ORDER_TYPE].str.contains("STOCK", case=False, na=False)
            
            def get_report_table(data):
                if data.empty: return None
                grp = data.groupby(COL_CUSTOMER).agg({
                    COL_BAG_NO: 'count',
                    COL_METAL_18KT: 'sum',
                    COL_DIA_CTS: 'sum'
                }).reset_index()
                grp.columns = ['Customer Name', 'Ord Qty', 'Metal 18kt', 'Dia Cts']
                total_row = pd.DataFrame([{
                    'Customer Name': 'TOTAL',
                    'Ord Qty': grp['Ord Qty'].sum(),
                    'Metal 18kt': grp['Metal 18kt'].sum(),
                    'Dia Cts': grp['Dia Cts'].sum()
                }])
                final_df = pd.concat([grp, total_row], ignore_index=True)
                final_df['Metal 18kt'] = final_df['Metal 18kt'].apply(std_round)
                final_df['Dia Cts'] = final_df['Dia Cts'].map('{:,.2f}'.format)
                return final_df

            def display_section(title, data):
                st.markdown(f"### {title}")
                table = get_report_table(data)
                if table is not None:
                    st.table(table)
                else:
                    st.info(f"No data available for {title}")
                st.divider()

            gt_bags = df[COL_BAG_NO].count()
            gt_metal = std_round(df[COL_METAL_18KT].sum())
            gt_dia = df[COL_DIA_CTS].sum()
            st.markdown(f"""<div style="background-color:#1E1E1E; padding:25px; border-radius:10px; border:2px solid #4F4F4F; text-align:center; color: white;">
                <div style="font-size:28px; font-weight:bold;">{gt_bags} Ord Qty | {gt_metal} Metal 18kt | {gt_dia:,.2f} Dia Cts</div></div>""", unsafe_allow_html=True)
            st.write("")
            display_section("Customer Orders", df[is_cust])
            display_section("Stock Orders", df[is_stock])
            display_section("Metal Issued Customer Orders", df[issued_mask & is_cust])
            display_section("Metal Pending Customer Orders", df[~issued_mask & is_cust])
            display_section("Metal Issued Stock Orders", df[issued_mask & is_stock])
            display_section("Metal Pending Stock Orders", df[~issued_mask & is_stock])

        # ═══════════════════════════════════════════════════════════════
        # 🔍 BAG HISTORY REPORT
        # ═══════════════════════════════════════════════════════════════
        elif menu == "🔍 Bag History Report":
            st.header("🔍 Bag History Report")
            search_bag = st.text_input("Enter Bag Number to Search").strip()
            
            if search_bag:
                match = df[df[COL_BAG_NO].astype(str).str.upper() == search_bag.upper()]
                if not match.empty:
                    r = match.iloc[0]
                    col_det, col_img = st.columns([2, 1])
                    with col_det:
                        st.markdown("### 📦 Bag Master Details")
                        sub1, sub2 = st.columns(2)
                        with sub1:
                            st.write(f"**Customer:** {safe_get(r, COL_CUSTOMER, 'N/A')}")
                            st.write(f"**Type:** {safe_get(r, COL_ORDER_TYPE, 'N/A')}")
                            st.write(f"**Karigar:** {safe_get(r, COL_KARIGAR, 'N/A')}")
                            st.write(f"**Metal:** {std_round(safe_get(r, COL_METAL_18KT, 0))}g 18kt")
                            st.write(f"**Dia:** {float(safe_get(r, COL_DIA_CTS, 0)):.2f} Cts")
                            st.write(f"**Form:** {safe_get(r, COL_FORM, 'N/A')}")
                            st.write(f"**Metal Colour:** {safe_get(r, COL_METAL_COLOUR, 'N/A')}")
                        with sub2:
                            st.write(f"**Ordered:** {clean_date(safe_get(r, COL_ORDER_DATE))}")
                            st.write(f"**Metal Iss:** {clean_date(safe_get(r, COL_METAL_ISSUE_DATE))}")
                            st.write(f"**Deliv Dt:** {clean_date(safe_get(r, COL_DELIVERY_DATE))}")
                            st.write(f"**Status:** {safe_get(r, COL_STATUS, 'N/A')}")
                            st.write(f"**Status Date:** {safe_get(r, COL_STATUS_DATE)}")
                            st.write(f"**Karigar DOD:** {clean_date(safe_get(r, COL_KARIGAR_DOD))}")
                    
                    with col_img:
                        st.markdown("### 🖼️ Design")
                        img_url = r.get(COL_IMAGE_LINK)
                        if img_url and str(img_url).strip() not in ["", "---", "None", "nan"]:
                            if "id=" in str(img_url):
                                file_id = str(img_url).split("id=")[1].split("&")[0]
                            elif "d/" in str(img_url):
                                file_id = str(img_url).split("d/")[1].split("/")[0]
                            else:
                                file_id = None
                            if file_id:
                                thumb_url = f"https://lh3.googleusercontent.com/u/0/d/{file_id}"
                                st.markdown(f'<a href="{img_url}" target="_blank"><img src="{thumb_url}" width="100%" style="border-radius:10px; border:1px solid #4F4F4F;"></a>', unsafe_allow_html=True)
                                st.caption("👆 Click to enlarge")
                        else:
                            st.info("No Image")
                    
                    st.divider()
                    st.header("📋 QC Process Report")
                    
                    q1, q2, q3 = st.columns(3)
                    with q1:
                        st.markdown("**🛠️ GHAT DETAILS**")
                        st.write(f"**QC:** {safe_get(r, COL_GHAT_QC)}")
                        st.write(f"**Weight:** {float(safe_get(r, COL_GHAT_WT, 0)):.3f}g")
                        st.write(f"**Remark:** {safe_get(r, COL_GHAT_REMARK)}")
                        st.write(f"**Date:** {safe_get(r, COL_GHAT_DATE)}")
                        st.write(f"**C.S. 1st Issue:** {safe_get(r, COL_CS_1ST_ISSUER)} ({safe_get(r, COL_CS_1ST_QTY, 0)} pcs)")
                        st.write(f"**C.S. 1st Date:** {safe_get(r, COL_CS_1ST_DATE)}")
                    with q2:
                        st.markdown("**💎 SETTING DETAILS**")
                        st.write(f"**QC:** {safe_get(r, COL_SETTING_QC)}")
                        st.write(f"**Weight:** {float(safe_get(r, COL_SETTING_WT, 0)):.3f}g")
                        st.write(f"**Date:** {safe_get(r, COL_SETTING_DATE)}")
                        st.write(f"**C.S. 2nd Issue:** {safe_get(r, COL_CS_2ND_ISSUER)} ({safe_get(r, COL_CS_2ND_QTY, 0)} pcs)")
                        st.write(f"**C.S. 2nd Date:** {safe_get(r, COL_CS_2ND_DATE)}")
                    with q3:
                        st.markdown("**✨ FINAL FINISH**")
                        st.write(f"**Final QC:** {safe_get(r, COL_FINAL_QC)}")
                        st.write(f"**Final Wt:** {float(safe_get(r, COL_FINAL_WT, 0)):.3f}g")
                        st.write(f"**Remark:** {safe_get(r, COL_FINAL_QC_REMARK)}")
                        st.write(f"**QC Date:** {safe_get(r, COL_FINAL_QC_DATE)}")
                        st.write(f"**Finish:** {'✅ Yes' if safe_get(r, COL_FINISH) == True else '❌ No'}")
                        st.write(f"**Finish Date:** {safe_get(r, COL_FINISH_DATE)}")
                        st.write(f"**IGI/SGL:** {safe_get(r, COL_IGI_SGL)} | {safe_get(r, COL_IGI_DATE)}")
                        st.write(f"**HUID:** {'✅ Yes' if safe_get(r, COL_HUID) == True else '❌ No'} | {safe_get(r, COL_HUID_DATE)}")

                    st.divider()
                    try:
                        def get_movement_data(table_id):
                            query = f"SELECT * FROM `jewelry-sql-system.workshop_data.{table_id}` WHERE CAST(BAG_NO AS STRING) = '{search_bag}'"
                            m_df = client.query(query).to_dataframe()
                            if m_df.empty:
                                return m_df
                            m_df.columns = [str(c).strip().upper().replace(' ', '_').replace('.', '_').replace('/', '_') for c in m_df.columns]
                            date_col = next((c for c in m_df.columns if 'DATE' in c), None)
                            time_col = next((c for c in m_df.columns if 'TIME' in c), None)
                            if date_col:
                                m_df['SORT_DATE'] = pd.to_datetime(m_df[date_col], dayfirst=True, errors='coerce')
                                if time_col:
                                    m_df['SORT_TIME'] = pd.to_datetime(m_df[time_col], format='%I:%M %p', errors='coerce').dt.time
                                    m_df = m_df.sort_values(by=['SORT_DATE', 'SORT_TIME'], ascending=True)
                                else:
                                    m_df = m_df.sort_values(by='SORT_DATE', ascending=True)
                            for c in m_df.columns:
                                if 'DATE' in c and c != 'SORT_DATE':
                                    m_df[c] = pd.to_datetime(m_df[c], dayfirst=True, errors='coerce').dt.strftime('%d/%m/%Y')
                            return m_df.drop(columns=['SORT_DATE', 'SORT_TIME'], errors='ignore')

                        st.markdown("### 🛠️ PRE-FINISH MOVEMENT")
                        df_pre = get_movement_data("pre_finish_movement_native")
                        c1, c2 = st.columns(2)
                        with c1:
                            st.markdown('<p style="background-color:#E8F0FE; padding:8px; border-radius:5px; color:black; font-weight:bold;">Inward</p>', unsafe_allow_html=True)
                            if not df_pre.empty:
                                in_cols = [c for c in df_pre.columns if ('IN' in c or 'PURPOSE' in c) and 'OUT' not in c and 'BAG' not in c]
                                if in_cols:
                                    st.dataframe(df_pre[in_cols].dropna(how='all'), hide_index=True, use_container_width=True)
                        with c2:
                            st.markdown('<p style="background-color:#FEE8E8; padding:8px; border-radius:5px; color:black; font-weight:bold;">Outward</p>', unsafe_allow_html=True)
                            if not df_pre.empty:
                                out_cols = [c for c in df_pre.columns if 'OUT' in c and 'BAG' not in c]
                                if out_cols:
                                    st.dataframe(df_pre[out_cols].dropna(how='all'), hide_index=True, use_container_width=True)

                        st.write("")
                        st.markdown("### ✨ POST-FINISH MOVEMENT")
                        df_post = get_movement_data("post_finish_movement_native")
                        c3, c4 = st.columns(2)
                        with c3:
                            st.markdown('<p style="background-color:#FEE8E8; padding:8px; border-radius:5px; color:black; font-weight:bold;">Outward</p>', unsafe_allow_html=True)
                            if not df_post.empty:
                                out_cols_p = [c for c in df_post.columns if 'OUT' in c and 'BAG' not in c]
                                if out_cols_p:
                                    st.dataframe(df_post[out_cols_p].dropna(how='all'), hide_index=True, use_container_width=True)
                        with c4:
                            st.markdown('<p style="background-color:#E8F0FE; padding:8px; border-radius:5px; color:black; font-weight:bold;">Inward</p>', unsafe_allow_html=True)
                            if not df_post.empty:
                                in_cols_p = [c for c in df_post.columns if ('IN' in c or 'PURPOSE' in c) and 'OUT' not in c and 'BAG' not in c]
                                if in_cols_p:
                                    st.dataframe(df_post[in_cols_p].dropna(how='all'), hide_index=True, use_container_width=True)
                    except Exception as mv_e:
                        st.error(f"Movement Log Error: {mv_e}")
                else:
                    st.warning(f"Bag No {search_bag} not found.")

        # ═══════════════════════════════════════════════════════════════
        # 💰 SALES ANALYTICS
        # ═══════════════════════════════════════════════════════════════
        elif menu == "💰 Sales Analytics":
            st.header("💎 Sales Analytics")
            sdf = fetch_sales_data()
            
            if sdf is None:
                st.error("Could not fetch sales data.")
                st.stop()
            
            # Auto-detect sales columns
            sales_cols = [str(c).strip().upper().replace(' ', '_').replace('.', '_') for c in sdf.columns]
            sdf.columns = sales_cols
            
            _sales_customer = find_col(sdf, 'CUSTOMER', 'CUSTOMER_1', 'CUST_NAME')
            _sales_karigar = find_col(sdf, 'KARIGAR', 'KARIGAR_1')
            _sales_dia = find_col(sdf, 'DIA_CTS', 'DIA_CTS_1', 'DIAMOND_CTS', 'DIAMOND_CARAT')
            if _sales_dia is None:
                for c in sales_cols:
                    if 'DIA' in c or 'DIAMOND' in c:
                        try:
                            if pd.to_numeric(sdf[c], errors='coerce').notna().sum() > len(sdf) * 0.3:
                                _sales_dia = c
                                break
                        except:
                            pass
            _sales_date = find_col(sdf, 'DATE', 'ORDER_DATE', 'SALE_DATE', 'SALES_DATE')
            
            detected = {
                'Customer': _sales_customer,
                'Karigar': _sales_karigar,
                'Diamond Cts': _sales_dia,
                'Date': _sales_date
            }
            
            missing_sales = [k for k, v in detected.items() if v is None or v not in sales_cols]
            
            if missing_sales:
                st.error("❌ Could not auto-detect required Sales columns")
                st.error(f"Missing: {', '.join(missing_sales)}")
                st.write("**Available columns:**")
                st.code(", ".join(sales_cols))
                st.info("Please check your SALE_DATA_native table schema.")
                st.stop()
            
            st.success(f"✅ Auto-detected columns: {detected}")
            
            try:
                import plotly.express as px
                
                s_report = pd.DataFrame({
                    'Customer': sdf[_sales_customer].astype(str).str.strip(),
                    'Karigar': sdf[_sales_karigar].astype(str).str.strip() if _sales_karigar else 'Unknown',
                    'Dia_Cts': pd.to_numeric(sdf[_sales_dia], errors='coerce').fillna(0),
                    'Date': pd.to_datetime(sdf[_sales_date], dayfirst=True, errors='coerce')
                })

                s_report = s_report.dropna(subset=['Date'])
                s_report = s_report[s_report['Date'].dt.year == 2026]
                s_report = s_report[~s_report['Customer'].isin(["None", "nan", ""])]

                if not s_report.empty:
                    s_report['Month'] = s_report['Date'].dt.strftime('%B')
                    month_order = ['January', 'February', 'March', 'April', 'May', 'June',
                                   'July', 'August', 'September', 'October', 'November', 'December']
                    
                    st.subheader("👥 Customer Sales (Month-wise)")
                    cust_data = s_report.groupby(['Month', 'Customer'], observed=True)['Dia_Cts'].sum().reset_index()
                    
                    fig_cust = px.bar(
                        cust_data,
                        x="Month",
                        y="Dia_Cts",
                        color="Customer",
                        barmode="group",
                        text_auto='.2f',
                        category_orders={"Month": month_order},
                        template="plotly_dark"
                    )
                    fig_cust.update_layout(yaxis_title="Diamond Cts", xaxis_title="")
                    st.plotly_chart(fig_cust, use_container_width=True)

                    st.divider()

                    st.subheader("⚒️ Karigar Production (Month-wise)")
                    karigar_data = s_report.groupby(['Month', 'Karigar'], observed=True)['Dia_Cts'].sum().reset_index()
                    
                    fig_kari = px.bar(
                        karigar_data,
                        x="Month",
                        y="Dia_Cts",
                        color="Karigar",
                        barmode="group",
                        text_auto='.2f',
                        category_orders={"Month": month_order},
                        template="plotly_dark"
                    )
                    fig_kari.update_layout(yaxis_title="Diamond Cts", xaxis_title="")
                    st.plotly_chart(fig_kari, use_container_width=True)

                    st.divider()

                    st.subheader("📋 Monthly Detailed Breakdown")
                    s_report['Month_Year'] = s_report['Date'].dt.strftime('%b-%y')
                    unique_months = s_report.sort_values('Date', ascending=False)['Month_Year'].unique()

                    for month in unique_months:
                        with st.expander(f"📅 Details for {month}"):
                            m_data = s_report[s_report['Month_Year'] == month]
                            summary = m_data.groupby('Customer').agg({'Dia_Cts': 'sum'}).reset_index()
                            t_row = pd.DataFrame([{'Customer': 'TOTAL', 'Dia_Cts': summary['Dia_Cts'].sum()}])
                            final = pd.concat([summary, t_row], ignore_index=True)
                            final['Dia cts'] = final['Dia_Cts'].map('{:,.2f}'.format)
                            st.table(final[['Customer', 'Dia cts']])
                else:
                    st.info("No sales records found for 2026.")

            except ImportError:
                st.error("Missing 'plotly' module. Please add it to requirements.txt.")
            except Exception as e:
                st.error(f"Analytics Error: {e}")
