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

        # --- REPORT 1: METAL REQUIREMENTS (Existing) ---
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

        # --- REPORT 2: CSR (Added Totals) ---
        elif menu == "📋 CSR":
            st.header("📋 Customer Status Report")
            status_seq = {"SEQUENCE": 0, "ENGRAVING/HUID": 1, "IGI": 2, "ON HAND": 3, "FINAL QC": 4, "SETTING QC OK": 5, "SETTING": 6, "GHAT OK": 7, "CASTING": 8, "METAL ISSUED": 9, "METAL PENDING": 10, "HOLD": 12, "CANCEL": 13}
            csr_df = df.copy()
            csr_df['Seq'] = csr_df[col_status].map(status_seq).fillna(99)
            for cust in sorted(csr_df[col_cust].unique()):
                with st.expander(f"👤 CUSTOMER: {cust}"):
                    cust_data = csr_df[csr_df[col_cust] == cust]
                    summary = cust_data.groupby([col_status, 'Seq']).agg({col_bag: 'count', col_metal: 'sum', col_dia: 'sum'}).reset_index().sort_values('Seq')
                    
                    # Calculate Total Row
                    total_row = pd.DataFrame([{
                        col_status: 'TOTAL',
                        col_bag: summary[col_bag].sum(),
                        col_metal: summary[col_metal].sum(),
                        col_dia: summary[col_dia].sum()
                    }])
                    
                    final_summary = pd.concat([summary, total_row], ignore_index=True)
                    final_summary['Metal 18kt'] = final_summary[col_metal].apply(std_round)
                    final_summary['Dia Cts'] = final_summary[col_dia].map('{:,.2f}'.format)
                    
                    st.dataframe(final_summary[[col_status, col_bag, 'Metal 18kt', 'Dia Cts']].rename(columns={col_status: 'Status', col_bag: 'Bag Qty'}), hide_index=True, use_container_width=True)

        # --- REPORT: SCOPE OF WORK (Existing) ---
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
                    # --- 2. QC PROCESS REPORT (Corrected Columns) ---
                    st.markdown("### 📋 QC Process Report")
                    
                    def find_col(letter):
                        # BigQuery often appends underscores or 'COLUMN_' to single letter headers
                        potential_names = [letter, f"_{letter}_", f"COLUMN_{letter}", letter.upper()]
                        for name in potential_names:
                            if name in match.columns: return name
                        return None
                    
                    def get_smart_val(letter, default="---"):
                        col = find_col(letter)
                        if col and pd.notna(r[col]): 
                            val = r[col]
                            # If it's a numeric column (Weight), handle rounding/formatting
                            if letter in ['Y', 'AI', 'AL']:
                                try: return f"{float(val):.2f}"
                                except: return val
                            return val
                        return default

                    q1, q2, q3 = st.columns(3)
                    
                    with q1:
                        st.markdown("**🛠️ GHAT DETAILS**")
                        st.write(f"**QC:** {get_smart_val('X')}") # Column X
                        st.write(f"**Weight:** {get_smart_val('Y')}g") # Column Y
                        # Date using your existing GHAT_DATE logic but fallback to clean_date
                        st.write(f"**Date:** {clean_date(r.get('GHAT_DATE', '---'))}")

                    with q2:
                        st.markdown("**💎 SETTING DETAILS**")
                        st.write(f"**QC:** {get_smart_val('AH')}") # Column AH
                        st.write(f"**Weight:** {get_smart_val('AI')}g") # Column AI
                        st.write(f"**Date:** {clean_date(r.get('SETTING_DATE', '---'))}")

                    with q3:
                        st.markdown("**✨ FINAL FINISH**")
                        st.write(f"**Final QC:** {get_smart_val('AK')}") # Column AK
                        st.write(f"**Final Wt:** {get_smart_val('AL')}g") # Column AL
                        # QC Date from Column AN
                        st.write(f"**QC Date:** {clean_date(get_smart_val('AN'))}") # Column AN

                    st.divider()
                    # --- 3. MOVEMENT DATA LOGIC ---
                    try:
                        def get_movement_data(table_id):
                            query = f"SELECT * FROM `jewelry-sql-system.workshop_data.{table_id}` WHERE CAST(BAG_NO AS STRING) = '{search_bag}'"
                            m_df = client.query(query).to_dataframe()
                            if m_df.empty: return m_df
                            
                            m_df.columns = [str(c).strip().upper().replace(' ', '_').replace('.', '_') for c in m_df.columns]
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

                        # DISPLAY MOVEMENT TABLES
                        st.markdown("### 🛠️ PRE-FINISH MOVEMENT")
                        df_pre = get_movement_data("pre_finish_movement_native")
                        c1, c2 = st.columns(2)
                        with c1:
                            st.markdown('<p style="background-color:#E8F0FE; padding:8px; border-radius:5px; color:black; font-weight:bold;">Inward</p>', unsafe_allow_html=True)
                            if not df_pre.empty:
                                in_cols = [c for c in df_pre.columns if ('IN' in c or 'PURPOSE' in c) and 'OUT' not in c and 'BAG' not in c]
                                if in_cols: st.dataframe(df_pre[in_cols].dropna(how='all'), hide_index=True, use_container_width=True)
                        with c2:
                            st.markdown('<p style="background-color:#FEE8E8; padding:8px; border-radius:5px; color:black; font-weight:bold;">Outward</p>', unsafe_allow_html=True)
                            if not df_pre.empty:
                                out_cols = [c for c in df_pre.columns if 'OUT' in c and 'BAG' not in c]
                                if out_cols: st.dataframe(df_pre[out_cols].dropna(how='all'), hide_index=True, use_container_width=True)

                        st.write("") 
                        st.markdown("### ✨ POST-FINISH MOVEMENT")
                        df_post = get_movement_data("post_finish_movement_native")
                        c3, c4 = st.columns(2)
                        with c3:
                            st.markdown('<p style="background-color:#FEE8E8; padding:8px; border-radius:5px; color:black; font-weight:bold;">Outward</p>', unsafe_allow_html=True)
                            if not df_post.empty:
                                out_cols_p = [c for c in df_post.columns if 'OUT' in c and 'BAG' not in c]
                                if out_cols_p: st.dataframe(df_post[out_cols_p].dropna(how='all'), hide_index=True, use_container_width=True)
                        with c4:
                            st.markdown('<p style="background-color:#E8F0FE; padding:8px; border-radius:5px; color:black; font-weight:bold;">Inward</p>', unsafe_allow_html=True)
                            if not df_post.empty:
                                in_cols_p = [c for c in df_post.columns if ('IN' in c or 'PURPOSE' in c) and 'OUT' not in c and 'BAG' not in c]
                                if in_cols_p: st.dataframe(df_post[in_cols_p].dropna(how='all'), hide_index=True, use_container_width=True)

                    except Exception as mv_e:
                        st.error(f"Movement Log Error: {mv_e}")
                else:
                    st.warning(f"Bag No {search_bag} not found.")

       # --- REPORT 4: SALES REPORT (Interactive Bar Graphs - Dia Cts) ---
        elif menu == "💰 Sales Report":
            st.header("💎 Sales Analytics")
            sdf = fetch_sales_data()
            
            if sdf is not None:
                try:
                    import plotly.express as px
                    
                    # 1. Data Prep (A=Cust, J=Karigar, L=Dia Cts, T=Date)
                    s_report = pd.DataFrame({
                        'Customer': sdf.iloc[:, 0].astype(str).str.strip(),
                        'Karigar': sdf.iloc[:, 9].astype(str).str.strip(), # Column J (Index 9)
                        'Dia_Cts': pd.to_numeric(sdf.iloc[:, 11], errors='coerce').fillna(0), # Column L (Index 11)
                        'Date': pd.to_datetime(sdf.iloc[:, 19], dayfirst=True, errors='coerce')
                    })

                    # Clean Ghost Rows & filter for 2026
                    s_report = s_report.dropna(subset=['Date'])
                    s_report = s_report[s_report['Date'].dt.year == 2026]
                    s_report = s_report[~s_report['Customer'].isin(["None", "nan", ""])]

                    if not s_report.empty:
                        # Prepare Months for X-axis
                        s_report['Month'] = s_report['Date'].dt.strftime('%B')
                        month_order = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
                        
                        # --- GRAPH 1: CUSTOMER DIA SALE (BAR) ---
                        st.subheader("👥 Customer Sales (Month-wise)")
                        cust_data = s_report.groupby(['Month', 'Customer'], observed=True)['Dia_Cts'].sum().reset_index()
                        
                        fig_cust = px.bar(
                            cust_data, 
                            x="Month", 
                            y="Dia_Cts", 
                            color="Customer",
                            barmode="group", # Side-by-side bars
                            text_auto='.2f', # Show 2 decimal places on top of bars
                            category_orders={"Month": month_order},
                            template="plotly_dark",
                            animation_frame=None # You can add "Month" here if you want a play button!
                        )
                        fig_cust.update_layout(yaxis_title="Diamond Cts", xaxis_title="")
                        st.plotly_chart(fig_cust, use_container_width=True)

                        st.divider()

                        # --- GRAPH 2: KARIGAR DIA PRODUCTION (BAR) ---
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

                        # --- MONTHLY DETAIL TABLES ---
                        st.subheader("📋 Monthly Detailed Breakdown")
                        s_report['Month_Year'] = s_report['Date'].dt.strftime('%b-%y')
                        unique_months = s_report.sort_values('Date', ascending=False)['Month_Year'].unique()

                        for month in unique_months:
                            with st.expander(f"📅 Details for {month}"):
                                m_data = s_report[s_report['Month_Year'] == month]
                                summary = m_data.groupby('Customer').agg({'Dia_Cts': 'sum'}).reset_index()
                                # Add TOTAL Row
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
