import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime

# ═══════════════════════════════════════════════════════════════
# COLUMN CONFIGURATION — Update if your schema changes
# ═══════════════════════════════════════════════════════════════
COL_CUSTOMER = 'CUSTOMER'
COL_ORDER_DATE = 'ORDER_DATE'
COL_ORDER_TYPE = 'ORDER_TYPE'
COL_STATUS = 'VZ STATUS'              # <-- CHANGE THIS if status is in a different column
COL_BAG_NO = 'BAG_NO'
COL_STYLE_NO = 'STYLE_NO'
COL_ITEM = 'ITEM'
COL_IMAGE = 'IMAGE'
COL_CAD_LINK = 'CAD_LINK'
COL_KARIGAR = 'KARIGAR'
COL_METAL_18KT = 'METAL_18KT'
COL_DIA_CTS = 'DIA_CTS'
COL_METAL_ISSUE_DATE = 'METAL_ISSUE_DATE'
COL_DIA_ISSUE_DATE = 'DIA_ISSUE_DATE'
COL_KARIGAR_DOD = 'KARIGAR_DOD'
COL_ESTIMATED_DOD = 'ESTIMATED_DOD'
COL_IGI_SGL = 'IGI_SGL'
COL_FINISH_DATE = 'FINISH_DATE'
COL_DELIVERY = 'DELIVERY'
COL_DELIVERY_DATE = 'DELIVERY_DATE'

# Sales table column config (will auto-detect or fall back)
SALES_COL_CUSTOMER = None       # Auto-detect
SALES_COL_KARIGAR = None        # Auto-detect
SALES_COL_DIA_CTS = None        # Auto-detect
SALES_COL_DATE = None           # Auto-detect

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
        # Normalize column names to uppercase with underscores
        df.columns = [str(c).strip().upper().replace(' ', '_').replace('.', '_').replace('/', '_') for c in df.columns]
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
        return dt.strftime('%d-%b-%Y')
    except: return str(dt)

def safe_get(row, col, default="---"):
    try:
        val = row.get(col)
        if pd.isna(val) or str(val).strip() in ["", "None", "nan"]: return default
        return val
    except:
        return default

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
            st.info(f"💡 Update `COL_STATUS` (currently set to '{COL_STATUS}') if your status column has a different name.")
            st.stop()

        # Convert numeric columns
        df[COL_METAL_18KT] = pd.to_numeric(df[COL_METAL_18KT], errors='coerce').fillna(0)
        df[COL_DIA_CTS] = pd.to_numeric(df[COL_DIA_CTS], errors='coerce').fillna(0)

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
            
            st.subheader("📋 master_inventory_native Columns")
            st.code(", ".join(df.columns.tolist()), language="text")
            
            st.subheader("📊 Data Types")
            st.dataframe(df.dtypes.to_frame(name="Data_Type").reset_index().rename(columns={"index": "Column"}),
                        use_container_width=True, hide_index=True)
            
            st.subheader("🔍 First 3 Rows")
            st.dataframe(df.head(3), use_container_width=True)
            
            # Check status column values
            if COL_STATUS in df.columns:
                st.subheader(f"🔍 Unique Values in '{COL_STATUS}' (Status Column)")
                status_counts = df[COL_STATUS].value_counts().reset_index()
                status_counts.columns = [COL_STATUS, 'Count']
                st.dataframe(status_counts, use_container_width=True, hide_index=True)
            
            # Sales diagnostics
            st.divider()
            st.subheader("📋 SALE_DATA_native Columns")
            sdf = fetch_sales_data()
            if sdf is not None:
                sales_cols = sdf.columns.tolist()
                st.write(f"**Total Columns:** {len(sales_cols)}")
                for i in range(0, len(sales_cols), 10):
                    st.code(", ".join(sales_cols[i:i+10]), language="text")
                st.subheader("🔍 First 3 Rows")
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
                        with sub2:
                            st.write(f"**Ordered:** {clean_date(safe_get(r, COL_ORDER_DATE))}")
                            st.write(f"**Metal Iss:** {clean_date(safe_get(r, COL_METAL_ISSUE_DATE))}")
                            st.write(f"**Deliv Dt:** {clean_date(safe_get(r, COL_DELIVERY_DATE))}")
                            st.write(f"**Status:** {safe_get(r, COL_STATUS, 'N/A')}")
                    
                    with col_img:
                        st.markdown("### 🖼️ Design")
                        img_url = r.get(COL_IMAGE)
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
                    
                    def get_val_flex(prefix):
                        col = next((c for c in match.columns if c.startswith(prefix)), None)
                        if col:
                            val = r[col]
                            if pd.notna(val) and str(val).strip() not in ["", "None", "nan"]:
                                return val
                        return "---"

                    def get_wt_flex(prefix):
                        col = next((c for c in match.columns if c.startswith(prefix)), None)
                        if col:
                            val = r[col]
                            try:
                                v = float(val)
                                return f"{v:.2f}" if v > 0 else "0.00"
                            except:
                                return "0.00"
                        return "0.00"

                    def get_date_flex(prefix):
                        val = get_val_flex(prefix)
                        if val == "---":
                            return "---"
                        try:
                            dt = pd.to_datetime(val, dayfirst=True, errors='coerce')
                            return dt.strftime('%d/%m/%Y %I:%M %p') if pd.notnull(dt) else str(val)
                        except:
                            return str(val)

                    q1, q2, q3 = st.columns(3)
                    with q1:
                        st.markdown("**🛠️ GHAT DETAILS**")
                        st.write(f"**QC:** {get_val_flex('GHAT_QC')}")
                        st.write(f"**Weight:** {get_wt_flex('GHAT_WT')}g")
                        st.write(f"**Date:** {get_date_flex('GHAT_DATE')}")
                    with q2:
                        st.markdown("**💎 SETTING DETAILS**")
                        st.write(f"**QC:** {get_val_flex('SETTING_QC')}")
                        st.write(f"**Weight:** {get_wt_flex('SETTING_WT')}g")
                        st.write(f"**Date:** {get_date_flex('SETTING_DATE')}")
                    with q3:
                        st.markdown("**✨ FINAL FINISH**")
                        st.write(f"**Final QC:** {get_val_flex('FINAL_QC')}")
                        st.write(f"**Final Wt:** {get_wt_flex('FINAL_WT')}g")
                        st.write(f"**QC Date:** {get_date_flex('FINAL_QC_DATE')}")

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
            
            global SALES_COL_CUSTOMER, SALES_COL_KARIGAR, SALES_COL_DIA_CTS, SALES_COL_DATE
            
            if SALES_COL_CUSTOMER is None:
                SALES_COL_CUSTOMER = next((c for c in sales_cols if 'CUSTOMER' in c and 'DETAIL' not in c and 'SECTION' not in c), None)
            if SALES_COL_KARIGAR is None:
                SALES_COL_KARIGAR = next((c for c in sales_cols if 'KARIGAR' in c), None)
            if SALES_COL_DIA_CTS is None:
                SALES_COL_DIA_CTS = next((c for c in sales_cols if 'DIA' in c and ('CTS' in c or 'CARAT' in c or 'CT' in c)), None)
            if SALES_COL_DATE is None:
                SALES_COL_DATE = next((c for c in sales_cols if 'DATE' in c and 'ISSUE' not in c and 'DOD' not in c and 'FINISH' not in c and 'DELIVERY' not in c), None)
            
            # If still not found, try common alternatives
            if SALES_COL_CUSTOMER is None:
                SALES_COL_CUSTOMER = next((c for c in sales_cols if 'CUST' in c), None)
            if SALES_COL_DIA_CTS is None:
                # Try numeric columns that might be diamond cts
                for c in sales_cols:
                    if 'DIA' in c or 'DIAMOND' in c:
                        try:
                            if pd.to_numeric(sdf[c], errors='coerce').notna().sum() > len(sdf) * 0.5:
                                SALES_COL_DIA_CTS = c
                                break
                        except:
                            pass
            if SALES_COL_DATE is None:
                SALES_COL_DATE = next((c for c in sales_cols if 'DATE' in c), None)
            
            # Show what we detected
            detected = {
                'Customer': SALES_COL_CUSTOMER,
                'Karigar': SALES_COL_KARIGAR,
                'Diamond Cts': SALES_COL_DIA_CTS,
                'Date': SALES_COL_DATE
            }
            
            missing_sales = [k for k, v in detected.items() if v is None or v not in sales_cols]
            
            if missing_sales:
                st.error("❌ Could not auto-detect required Sales columns")
                st.error(f"Missing: {', '.join(missing_sales)}")
                st.write("**Available columns:**")
                st.code(", ".join(sales_cols))
                st.info("Please specify the column names in the SALES_COL_* variables at the top of the code, or share a sample of your sales data.")
                st.stop()
            
            st.success(f"✅ Auto-detected columns: {detected}")
            
            try:
                import plotly.express as px
                
                s_report = pd.DataFrame({
                    'Customer': sdf[SALES_COL_CUSTOMER].astype(str).str.strip(),
                    'Karigar': sdf[SALES_COL_KARIGAR].astype(str).str.strip() if SALES_COL_KARIGAR else 'Unknown',
                    'Dia_Cts': pd.to_numeric(sdf[SALES_COL_DIA_CTS], errors='coerce').fillna(0),
                    'Date': pd.to_datetime(sdf[SALES_COL_DATE], dayfirst=True, errors='coerce')
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
