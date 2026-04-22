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
        if isinstance(dt, str): dt = pd.to_datetime(dt, dayfirst=True)
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

    if df is not None:
        col_metal = next((c for c in df.columns if 'METAL' in c and '18' in c and 'WT' in c), 'METAL_18KT_WT')
        col_status = next((c for c in df.columns if 'STATUS' in c and 'DATE' not in c), 'CURRENT_STATUS')
        col_cust = next((c for c in df.columns if 'CUSTOMER' in c), 'CUSTOMER')
        col_order_type = next((c for c in df.columns if 'ORDER_TYPE' in c), 'ORDER_TYPE')
        col_bag = next((c for c in df.columns if 'BAG' in c), 'BAG_NO')
        col_dia = next((c for c in df.columns if 'DIA' in c and 'CTS' in c), 'DIA_CTS')
        col_issue_dt = next((c for c in df.columns if 'METAL' in c and 'ISSUE' in c and 'DATE' in c), 'METAL_ISSUE_DATE')
        col_dia_issue = next((c for c in df.columns if 'DIA' in c and 'ISSUE' in c and 'DATE' in c and '2ND' not in c), 'DIA_ISSUE_DATE')

        df[col_metal] = pd.to_numeric(df[col_metal], errors='coerce').fillna(0)
        df[col_dia] = pd.to_numeric(df[col_dia], errors='coerce').fillna(0)

        # --- SIDEBAR ---
        st.sidebar.markdown("### 📊 MAIN REPORTS")
        menu = st.sidebar.radio("SELECT REPORT", ["📊 Metal Requirements", "📋 CSR", "📋 Scope of Work", "🔍 Bag History Report", "💰 Sales Analytics"], label_visibility="collapsed")
        
        st.sidebar.divider()
        st.sidebar.markdown("### 🚨 DELAY REPORTS")
        delay_menu = st.sidebar.radio("SELECT DELAY REPORT", ["None", "🕒 CAD Delay Report", "🕒 Ghat Delay Report"], label_visibility="collapsed")

        active_report = delay_menu if delay_menu != "None" else menu

        st.sidebar.divider()
        if st.sidebar.button("🔄 REFRESH MOVEMENT DATA"):
            with st.sidebar.spinner("Syncing..."):
                refresh_native_tables()

        # --- GHAT DELAY REPORT LOGIC ---
        if active_report == "🕒 Ghat Delay Report":
            st.header("🕒 Ghat Delay Report")
            st.info("Logic: Metal Issued but Dia Not Issued. Delay > 5 days (<= 5cts) or > 9 days (> 5cts).")
            
            ghat_df = df.copy()
            ghat_df['METAL_ISSUE_DT'] = pd.to_datetime(ghat_df[col_issue_dt], dayfirst=True, errors='coerce')
            
            # 1. Metal Issued is NOT blank
            # 2. Dia Issued IS blank
            mask = (ghat_df['METAL_ISSUE_DT'].notna()) & \
                   (ghat_df[col_dia_issue].isna() | (ghat_df[col_dia_issue].astype(str).str.strip() == ""))
            
            ghat_delay = ghat_df[mask].copy()
            today = datetime.now()
            ghat_delay['DELAY_DAYS'] = (today - ghat_delay['METAL_ISSUE_DT']).dt.days
            
            # Apply product size logic
            small_mask = (ghat_delay[col_dia] <= 5) & (ghat_delay['DELAY_DAYS'] > 5)
            big_mask = (ghat_delay[col_dia] > 5) & (ghat_delay['DELAY_DAYS'] > 9)
            
            final_ghat = ghat_delay[small_mask | big_mask].sort_values('DELAY_DAYS', ascending=False)

            if not final_ghat.empty:
                # --- FILTERS ---
                st.write("#### 🔍 Filter Results")
                f1, f2, f3 = st.columns(3)
                with f1:
                    sel_cust = st.multiselect("Filter by Customer", sorted(final_ghat[col_cust].unique()))
                with f2:
                    sel_karigar = st.multiselect("Filter by Karigar", sorted(final_ghat['KARIGAR'].astype(str).unique()))
                with f3:
                    min_d, max_d = final_ghat['METAL_ISSUE_DT'].min(), final_ghat['METAL_ISSUE_DT'].max()
                    date_range = st.date_input("Metal Issue Date Range", [min_d, max_d])

                if sel_cust: final_ghat = final_ghat[final_ghat[col_cust].isin(sel_cust)]
                if sel_karigar: final_ghat = final_ghat[final_ghat['KARIGAR'].astype(str).isin(sel_karigar)]
                if len(date_range) == 2:
                    final_ghat = final_ghat[(final_ghat['METAL_ISSUE_DT'].dt.date >= date_range[0]) & (final_ghat['METAL_ISSUE_DT'].dt.date <= date_range[1])]

                # --- DISPLAY ---
                cols = st.columns([1, 1.2, 1, 1.2, 0.8, 1, 1.5])
                headers = ["Customer", "Order Date", "Bag No", "Metal Issue", "Delay", "Karigar", "Design"]
                for col, text in zip(cols, headers): col.markdown(f"**{text}**")
                st.divider()

                for _, row in final_ghat.iterrows():
                    c1, c2, c3, c4, c5, c6, c7 = st.columns([1, 1.2, 1, 1.2, 0.8, 1, 1.5])
                    c1.write(row[col_cust])
                    c2.write(clean_date(row['ORDER_DATE']))
                    c3.write(f"**{row[col_bag]}**")
                    c4.write(clean_date(row[col_issue_dt]))
                    c5.write(f"🕒 {int(row['DELAY_DAYS'])}d")
                    c6.write(row.get('KARIGAR', '---'))
                    
                    # Thumbnail Logic
                    img_url = row.get('IMAGE_LINK')
                    if img_url and str(img_url).strip() not in ["", "---", "None"]:
                        file_id = str(img_url).split("id=")[1].split("&")[0] if "id=" in str(img_url) else (str(img_url).split("d/")[1].split("/")[0] if "d/" in str(img_url) else None)
                        if file_id:
                            thumb = f"https://lh3.googleusercontent.com/u/0/d/{file_id}"
                            c7.markdown(f'<a href="{img_url}" target="_blank"><img src="{thumb}" width="80px" style="border-radius:5px; border:1px solid #4F4F4F;"></a>', unsafe_allow_html=True)
                    else: c7.write("No Image")
                    st.divider()
            else:
                st.success("✅ No Ghat delays detected.")

        # --- CAD DELAY REPORT (REMAINS UNCHANGED) ---
        elif active_report == "🕒 CAD Delay Report":
            st.header("🕒 CAD Delay Report (Stock Orders)")
            cad_df = df.copy()
            cad_df['ORDER_DATE_DT'] = pd.to_datetime(cad_df['ORDER_DATE'], dayfirst=True, errors='coerce')
            mask = (cad_df[col_order_type].str.contains("STOCK", case=False, na=False)) & \
                   (cad_df['CAD'].isna() | (cad_df['CAD'].astype(str).str.strip() == "")) & \
                   (cad_df[col_issue_dt].isna() | (cad_df[col_issue_dt].astype(str).str.strip() == ""))
            delay_data = cad_df[mask].copy()
            today = datetime.now()
            delay_data['CAD_DELAY'] = (today - delay_data['ORDER_DATE_DT']).dt.days
            final_delay = delay_data[delay_data['CAD_DELAY'] > 5].sort_values('CAD_DELAY', ascending=False)
            
            if not final_delay.empty:
                st.write("#### 🔍 Filter Results")
                f1, f2, f3 = st.columns(3)
                with f1: sel_cust = st.multiselect("Filter by Customer", sorted(final_delay[col_cust].unique()))
                with f2: sel_karigar = st.multiselect("Filter by Karigar", sorted(final_delay['KARIGAR'].astype(str).unique()))
                with f3: date_range = st.date_input("Order Date Range", [final_delay['ORDER_DATE_DT'].min(), final_delay['ORDER_DATE_DT'].max()])
                
                if sel_cust: final_delay = final_delay[final_delay[col_cust].isin(sel_cust)]
                if sel_karigar: final_delay = final_delay[final_delay['KARIGAR'].astype(str).isin(sel_karigar)]
                if len(date_range) == 2:
                    final_delay = final_delay[(final_delay['ORDER_DATE_DT'].dt.date >= date_range[0]) & (final_delay['ORDER_DATE_DT'].dt.date <= date_range[1])]

                h1, h2, h3, h4, h5, h6, h7 = st.columns([1.2, 1, 1.2, 1, 0.8, 1, 1.5])
                for col, text in zip([h1,h2,h3,h4,h5,h6,h7], ["Bag No", "Customer", "Order Date", "Type", "Delay", "Karigar", "Design"]): col.markdown(f"**{text}**")
                st.divider()

                for _, row in final_delay.iterrows():
                    c1, c2, c3, c4, c5, c6, c7 = st.columns([1.2, 1, 1.2, 1, 0.8, 1, 1.5])
                    c1.write(f"**{row[col_bag]}**")
                    c2.write(row[col_cust]); c3.write(clean_date(row['ORDER_DATE'])); c4.write(row[col_order_type])
                    c5.write(f"⚠️ {int(row['CAD_DELAY'])}d"); c6.write(row.get('KARIGAR', '---'))
                    img_url = row.get('IMAGE_LINK')
                    if img_url and str(img_url).strip() not in ["", "---", "None"]:
                        file_id = str(img_url).split("id=")[1].split("&")[0] if "id=" in str(img_url) else (str(img_url).split("d/")[1].split("/")[0] if "d/" in str(img_url) else None)
                        if file_id:
                            thumb = f"https://lh3.googleusercontent.com/u/0/d/{file_id}"
                            c7.markdown(f'<a href="{img_url}" target="_blank"><img src="{thumb}" width="80px" style="border-radius:5px; border:1px solid #4F4F4F;"></a>', unsafe_allow_html=True)
                    st.divider()

        # --- OTHER REPORTS (REMAINS UNCHANGED) ---
        elif active_report == "📊 Metal Requirements":
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
                    st.markdown(f"**SUBTOTAL:** {sub_data[col_bag].count()} Bags | {std_round(sub_data[col_metal].sum())}g 18kt | {sub_data[col_dia].sum():,.2f} Dia Cts")

        elif active_report == "📋 CSR":
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

        elif active_report == "📋 Scope of Work":
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
                final_df['Metal 18kt'] = final_df['Metal 18kt'].apply(std_round); final_df['Dia Cts'] = final_df['Dia Cts'].map('{:,.2f}'.format)
                return final_df
            st.markdown(f"""<div style="background-color:#1E1E1E; padding:25px; border-radius:10px; border:2px solid #4F4F4F; text-align:center; color: white;">
                <div style="font-size:28px; font-weight:bold;">{df[col_bag].count()} Ord Qty | {std_round(df[col_metal].sum())} Metal 18kt | {df[col_dia].sum():,.2f} Dia Cts</div></div>""", unsafe_allow_html=True)
            for title, d in [("Customer Orders", df[is_cust]), ("Stock Orders", df[is_stock]), ("Metal Issued Customer", df[issued_mask & is_cust]), ("Metal Pending Customer", df[~issued_mask & is_cust])]:
                st.markdown(f"### {title}"); t = get_report_table(d)
                if t is not None: st.table(t)
                else: st.info("No data"); st.divider()

        elif active_report == "🔍 Bag History Report":
            st.header("🔍 Bag History Report")
            search_bag = st.text_input("Enter Bag Number").strip()
            if search_bag:
                match = df[df[col_bag].astype(str).str.upper() == search_bag.upper()]
                if not match.empty:
                    r = match.iloc[0]; c_det, c_img = st.columns([2, 1])
                    with c_det:
                        st.markdown("### 📦 Bag Master Details")
                        s1, s2 = st.columns(2)
                        with s1: st.write(f"**Customer:** {r.get(col_cust)}"); st.write(f"**Type:** {r.get(col_order_type)}"); st.write(f"**Metal:** {std_round(r.get(col_metal))}g")
                        with s2: st.write(f"**Ordered:** {clean_date(r.get('ORDER_DATE'))}"); st.write(f"**Metal Iss:** {clean_date(r.get(col_issue_dt))}"); st.write(f"**Status:** {r.get(col_status)}")
                    st.divider(); st.header("📋 QC Process Report")
                    def get_v(p):
                        col = next((c for c in match.columns if c.startswith(p)), None)
                        return r[col] if col and pd.notna(r[col]) else "---"
                    q1, q2, q3 = st.columns(3)
                    with q1: st.markdown("**🛠️ GHAT**"); st.write(f"QC: {get_v('GHAT_QC')}"); st.write(f"Wt: {get_v('GHAT_WT')}g")
                    with q2: st.markdown("**💎 SETTING**"); st.write(f"QC: {get_v('SETTING_QC')}"); st.write(f"Wt: {get_v('SETTING_WT')}g")
                    with q3: st.markdown("**✨ FINAL**"); st.write(f"QC: {get_v('FINAL_QC')}"); st.write(f"Wt: {get_v('FINAL_WT')}g")

         # --- REPORT 4: SALES Analytics (Interactive Bar Graphs - Dia Cts) ---
        elif menu == "💰 Sales Analytics":
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
