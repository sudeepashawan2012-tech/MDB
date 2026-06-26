import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import gspread

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


def load_sheet_to_bigquery(sheet_id, sheet_name, bq_table_id):
    """
    Reads data directly from Google Sheets and loads it into BigQuery.
    Handles duplicate headers, empty headers, and mixed datatypes.
    """
    try:
        gc = gspread.authorize(creds)
        worksheet = gc.open_by_key(sheet_id).worksheet(sheet_name)
        
        # Use get_all_values to handle duplicate/empty headers manually
        all_values = worksheet.get_all_values()
        if not all_values or len(all_values) < 2:
            st.sidebar.warning(f"Sheet '{sheet_name}' appears empty")
            return None
            
        raw_headers = all_values[0]
        
        # Clean and deduplicate headers
        seen = {}
        clean_headers = []
        for i, h in enumerate(raw_headers):
            h = str(h).strip().upper().replace(' ', '_').replace('.', '_').replace('/', '_')
            if not h:
                h = f"UNNAMED_COL_{i}"
            # Handle duplicates
            original_h = h
            counter = 1
            while h in seen:
                h = f"{original_h}_DUP{counter}"
                counter += 1
            seen[h] = True
            clean_headers.append(h)
        
        # Build dataframe from data rows
        data_rows = all_values[1:]
        df = pd.DataFrame(data_rows, columns=clean_headers)
        
        # Replace common empty/placeholder values with None
        df = df.replace({
            '': None, 'nan': None, 'None': None, 'NaN': None, 
            'NULL': None, 'N/A': None, 'n/a': None, '-': None, '--': None
        })
        
        # Convert object columns to string to avoid pyarrow mixed-type errors
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str).replace('None', '')
        
        # Upload to BigQuery (overwrite existing)
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True
        )
        job = client.load_table_from_dataframe(df, bq_table_id, job_config=job_config)
        job.result()
        
        return len(df)
        
    except Exception as e:
        error_msg = str(e)
        if "EXTERNAL" in error_msg and "not allowed for this operation" in error_msg:
            st.sidebar.info(f"{bq_table_id.split('.')[-1]} is EXTERNAL — skipping direct load")
        else:
            st.sidebar.error(f"Sheets load failed: {error_msg}")
        return None


def refresh_native_tables():
    """
    Loads fresh data DIRECTLY into _native tables from Google Sheets.
    Bypasses EXTERNAL source tables entirely.
    """
    try:
        sheets_config = st.secrets.get("sheets", {})
        refresh_count = 0
        
        # --- MASTER INVENTORY: Load from Sheets directly into _native ---
        if "master_inventory_sheet_id" in sheets_config and sheets_config["master_inventory_sheet_id"]:
            try:
                gc = gspread.authorize(creds)
                worksheet = gc.open_by_key(sheets_config["master_inventory_sheet_id"]).worksheet(
                    sheets_config.get("master_inventory_sheet_name", "Sheet1")
                )
                all_values = worksheet.get_all_values()
                raw_headers = all_values[0]
                
                # Clean headers
                seen = {}
                clean_headers = []
                for i, h in enumerate(raw_headers):
                    h = str(h).strip().upper().replace(' ', '_').replace('.', '_').replace('/', '_')
                    if not h:
                        h = f"UNNAMED_COL_{i}"
                    original_h = h
                    counter = 1
                    while h in seen:
                        h = f"{original_h}_DUP{counter}"
                        counter += 1
                    seen[h] = True
                    clean_headers.append(h)
                
                df_sheet = pd.DataFrame(all_values[1:], columns=clean_headers)
                df_sheet = df_sheet.replace({'': None, 'nan': None, 'None': None, 'NaN': None, 'NULL': None, 'N/A': None, 'n/a': None, '-': None, '--': None})
                for col in df_sheet.select_dtypes(include=['object']).columns:
                    df_sheet[col] = df_sheet[col].astype(str).replace('None', '')
                
                # Write DIRECTLY to _native table (bypassing the EXTERNAL source table)
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
                job = client.load_table_from_dataframe(df_sheet, "jewelry-sql-system.workshop_data.master_inventory_native", job_config=job_config)
                job.result()
                
                st.sidebar.success(f"✅ Master Inventory: {len(df_sheet)} rows from Sheets")
                refresh_count += 1
            except Exception as e:
                st.sidebar.error(f"❌ Master Inventory Sheets load failed: {e}")
        
        # --- SALE DATA: Load from Sheets directly into _native ---
        if "sale_data_sheet_id" in sheets_config and sheets_config["sale_data_sheet_id"]:
            try:
                gc = gspread.authorize(creds)
                worksheet = gc.open_by_key(sheets_config["sale_data_sheet_id"]).worksheet(
                    sheets_config.get("sale_data_sheet_name", "Sheet1")
                )
                all_values = worksheet.get_all_values()
                raw_headers = all_values[0]
                
                seen = {}
                clean_headers = []
                for i, h in enumerate(raw_headers):
                    h = str(h).strip().upper().replace(' ', '_').replace('.', '_').replace('/', '_')
                    if not h:
                        h = f"UNNAMED_COL_{i}"
                    original_h = h
                    counter = 1
                    while h in seen:
                        h = f"{original_h}_DUP{counter}"
                        counter += 1
                    seen[h] = True
                    clean_headers.append(h)
                
                df_sheet = pd.DataFrame(all_values[1:], columns=clean_headers)
                df_sheet = df_sheet.replace({'': None, 'nan': None, 'None': None, 'NaN': None, 'NULL': None, 'N/A': None, 'n/a': None, '-': None, '--': None})
                for col in df_sheet.select_dtypes(include=['object']).columns:
                    df_sheet[col] = df_sheet[col].astype(str).replace('None', '')
                
                job_config = bigquery.LoadJobConfig(write_displacement="WRITE_TRUNCATE", autodetect=True)
                job = client.load_table_from_dataframe(df_sheet, "jewelry-sql-system.workshop_data.SALE_DATA_native", job_config=job_config)
                job.result()
                
                st.sidebar.success(f"✅ Sale Data: {len(df_sheet)} rows from Sheets")
                refresh_count += 1
            except Exception as e:
                st.sidebar.error(f"❌ Sale Data Sheets load failed: {e}")
        
        # --- PRE-FINISH & POST-FINISH: Refresh from EXTERNAL source (no Sheets needed) ---
        # These are EXTERNAL tables linked to workshop scanning — just refresh their native copies
        try:
            client.query("""CREATE OR REPLACE TABLE `jewelry-sql-system.workshop_data.pre_finish_movement_native` 
               CLUSTER BY BAG_NO AS SELECT * FROM `jewelry-sql-system.workshop_data.pre_finish_movement`""").result()
            st.sidebar.success("✅ Pre-Finish native refreshed")
        except Exception as e:
            st.sidebar.info(f"ℹ️ Pre-Finish: {e}")
            
        try:
            client.query("""CREATE OR REPLACE TABLE `jewelry-sql-system.workshop_data.post_finish_movement_native` 
               CLUSTER BY BAG_NO AS SELECT * FROM `jewelry-sql-system.workshop_data.post_finish_movement`""").result()
            st.sidebar.success("✅ Post-Finish native refreshed")
        except Exception as e:
            st.sidebar.info(f"ℹ️ Post-Finish: {e}")
        
        # Clear cache and store timestamp
        st.cache_data.clear()
        st.session_state["last_refresh"] = datetime.now().strftime("%d-%b-%Y %H:%M:%S")
        
        if refresh_count == 0:
            st.sidebar.warning("⚠️ No Sheet IDs configured. Add them to secrets.toml")
        else:
            st.sidebar.success("🔄 Refresh complete! Reports now show latest data.")
            
    except Exception as e:
        st.sidebar.error(f"❌ Refresh Failed: {e}")

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


# ========== ROLE-BASED LOGIN SYSTEM ==========

USER_ROLES = {
    "FOLLOWUP": {
        "password": "followup123",
        "can_view_reports": True,
        "can_view_cad_delay": True,
        "ghat_access": "FOLLOWUP",
        "can_download": False,
        "can_edit": True,
        "is_mgmt_viewonly": False
    },
    "QC": {
        "password": "qc123",
        "can_view_reports": False,
        "can_view_cad_delay": False,
        "ghat_access": "QC",
        "can_download": False,
        "can_edit": True,
        "is_mgmt_viewonly": False
    },
    "BAGGING": {
        "password": "bagging123",
        "can_view_reports": False,
        "can_view_cad_delay": False,
        "ghat_access": "BAGGING",
        "can_download": False,
        "can_edit": True,
        "is_mgmt_viewonly": False
    },
    "ADMIN": {
        "password": "admin123",
        "can_view_reports": True,
        "can_view_cad_delay": True,
        "ghat_access": "ALL",
        "can_download": True,
        "can_edit": True,
        "is_mgmt_viewonly": False
    },
    "MGMT": {
        "password": "mgmt123",
        "can_view_reports": True,
        "can_view_cad_delay": True,
        "ghat_access": "ALL",
        "can_download": True,
        "can_edit": False,
        "is_mgmt_viewonly": True
    },
    "OWNER": {
        "password": "owner123",
        "can_view_reports": True,
        "can_view_cad_delay": True,
        "ghat_access": "ALL",
        "can_download": True,
        "can_edit": True,
        "is_mgmt_viewonly": False
    }
}

# Login screen
if "user_role" not in st.session_state:
    st.title("🔒 Workshop Login")
    
    selected_role = st.session_state.get("_selected_role", None)
    
    if not selected_role:
        cols = st.columns(3)
        role_names = list(USER_ROLES.keys())
        
        for idx, role in enumerate(role_names):
            with cols[idx % 3]:
                if st.button(f"🔑 {role}", use_container_width=True, key=f"role_{role}"):
                    st.session_state["_selected_role"] = role
                    st.rerun()
    
    if selected_role:
        st.markdown(f"### Enter password for **{selected_role}**")
        pwd = st.text_input("Password", type="password", key="pwd_input")
        
        col1, col2 = st.columns([1, 3])
        with col1:
            if st.button("🔙 Back"):
                st.session_state.pop("_selected_role", None)
                st.rerun()
        with col2:
            if st.button("✅ Login", type="primary"):
                if pwd == USER_ROLES[selected_role]["password"]:
                    st.session_state["user_role"] = selected_role
                    st.session_state["user_perms"] = USER_ROLES[selected_role]
                    st.rerun()
                else:
                    st.error("❌ Wrong password")
    
    st.stop()

# --- USER IS LOGGED IN ---
user_role = st.session_state["user_role"]
user_perms = st.session_state["user_perms"]

# Show who is logged in in sidebar
st.sidebar.markdown(f"### 👤 Logged in as: **{user_role}**")
if st.sidebar.button("🚪 Logout"):
    for key in ["user_role", "user_perms", "_selected_role", "last_refresh"]:
        st.session_state.pop(key, None)
    st.rerun()

df = fetch_data()

if df is not None:
    # Define shared column names
        # --- ROBUST COLUMN DETECTION ---
    # Show all columns for debugging
    with st.sidebar.expander("🔍 DEBUG: Sheet Columns Detected"):
        st.write("All columns in loaded data:", list(df.columns))
    
    # Metal column - try multiple patterns
    col_metal = next((c for c in df.columns if 'METAL' in c and '18' in c and 'WT' in c), None)
    if not col_metal:
        col_metal = next((c for c in df.columns if 'METAL' in c and 'WT' in c), None)
    if not col_metal:
        col_metal = next((c for c in df.columns if 'METAL' in c), None)
    if not col_metal:
        col_metal = 'METAL_18KT_WT'
        st.sidebar.warning(f"⚠️ Metal column not found. Using fallback. Available: {list(df.columns)}")

    # Status column
    col_status = next((c for c in df.columns if 'STATUS' in c and 'DATE' not in c), None)
    if not col_status:
        col_status = next((c for c in df.columns if 'STATUS' in c), 'CURRENT_STATUS')

    # Customer column
    col_cust = next((c for c in df.columns if 'CUSTOMER' in c), 'CUSTOMER')

    # Order Type column
    col_order_type = next((c for c in df.columns if 'ORDER_TYPE' in c), 'ORDER_TYPE')

    # Bag column
    col_bag = next((c for c in df.columns if 'BAG' in c), 'BAG_NO')

    # Diamond column - try multiple patterns
    col_dia = next((c for c in df.columns if 'DIA' in c and 'CTS' in c), None)
    if not col_dia:
        col_dia = next((c for c in df.columns if 'DIA' in c and 'CT' in c), None)
    if not col_dia:
        col_dia = next((c for c in df.columns if 'DIA' in c), 'DIA_CTS')

    # Metal Issue Date column
    col_issue_dt = next((c for c in df.columns if 'METAL' in c and 'ISSUE' in c and 'DATE' in c), 'METAL_ISSUE_DATE')

    # Safe numeric conversion
    if col_metal in df.columns:
        df[col_metal] = pd.to_numeric(df[col_metal], errors="coerce").fillna(0)
    else:
        st.sidebar.error(f"❌ Metal column '{col_metal}' NOT FOUND in data. Reports may show 0.")
        df[col_metal] = 0

    if col_dia in df.columns:
        df[col_dia] = pd.to_numeric(df[col_dia], errors="coerce").fillna(0)
    else:
        st.sidebar.error(f"❌ Dia column '{col_dia}' NOT FOUND in data. Reports may show 0.")
        df[col_dia] = 0

    
    # --- SIDEBAR NAVIGATION ---
    st.sidebar.markdown("### 📊 MAIN REPORTS")
    menu = st.sidebar.radio("SELECT REPORT", ["📊 Metal Requirements", "📋 CSR", "📋 Scope of Work", "🔍 Bag History Report", "💰 Sales Analytics"], label_visibility="collapsed")
    
    st.sidebar.markdown("### 🚨 DELAY REPORTS")
    delay_menu = st.sidebar.radio("SELECT DELAY REPORT", ["None", "🕒 CAD Delay Report", "🕒 Ghat Delay Report"], label_visibility="collapsed")

    # Determine which report to show
    active_report = delay_menu if delay_menu != "None" else menu

    st.sidebar.divider()
    
    # Show last refresh time
    last_refresh = st.session_state.get("last_refresh", "Never")
    st.sidebar.caption(f"Last refresh: {last_refresh}")
    st.sidebar.caption(f"Rows loaded: {len(df)}")

    if st.sidebar.button("🔄 REFRESH MOVEMENT DATA"):
        with st.sidebar.spinner("Syncing..."):
            refresh_native_tables()

    # --- REPORT logic ---

    if active_report == "🕒 CAD Delay Report":
        st.header("🕒 CAD Delay Report (Stock Orders)")
        st.info("Stock Orders: CAD is pending (> 5 days) AND Metal Issue is pending.")
        
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
            
            with f1:
                sel_cust = st.multiselect("Filter by Customer", sorted(final_delay[col_cust].unique()))
            with f2:
                sel_karigar = st.multiselect("Filter by Karigar", sorted(final_delay['KARIGAR'].astype(str).unique()))
            with f3:
                min_date = final_delay['ORDER_DATE_DT'].min().date()
                max_date = final_delay['ORDER_DATE_DT'].max().date()
                date_range = st.date_input(
                    "Filter by Order Date (DD/MM/YYYY)", 
                    [min_date, max_date],
                    format="DD/MM/YYYY"
                )
            
            if sel_cust:
                final_delay = final_delay[final_delay[col_cust].isin(sel_cust)]
            if sel_karigar:
                final_delay = final_delay[final_delay['KARIGAR'].astype(str).isin(sel_karigar)]
            if len(date_range) == 2:
                final_delay = final_delay[(final_delay['ORDER_DATE_DT'].dt.date >= date_range[0]) & 
                                          (final_delay['ORDER_DATE_DT'].dt.date <= date_range[1])]

            h1, h2, h3, h4, h5, h6, h7 = st.columns([1.2, 1, 1.2, 1, 0.8, 1, 1.5])
            h1.markdown("**Customer**")    
            h2.markdown("**Order Date**")
            h3.markdown("**Bag No**")
            h4.markdown("**Order Type**")
            h5.markdown("**Delay**")
            h6.markdown("**Karigar**")
            h7.markdown("**Design**")
            st.divider()

            for _, row in final_delay.iterrows():
                c1, c2, c3, c4, c5, c6, c7 = st.columns([1.2, 1, 1.2, 1, 0.8, 1, 1.5])
                c1.write(row[col_cust])
                c2.write(clean_date(row['ORDER_DATE']))
                c3.write(f"**{row[col_bag]}**")
                c4.write(row[col_order_type])
                c5.write(f"⚠️ {int(row['CAD_DELAY'])} Days")
                c6.write(row.get('KARIGAR', '---'))
                
                img_url = row.get('IMAGE_LINK')
                if img_url and str(img_url).strip() not in ["", "---", "None"]:
                    file_id = None
                    if "id=" in str(img_url): file_id = str(img_url).split("id=")[1].split("&")[0]
                    elif "d/" in str(img_url): file_id = str(img_url).split("d/")[1].split("/")[0]
                    
                    if file_id:
                        thumb_url = f"https://lh3.googleusercontent.com/u/0/d/{file_id}"
                        c7.markdown(f'<a href="{img_url}" target="_blank"><img src="{thumb_url}" width="80px" style="border-radius:5px; border:1px solid #4F4F4F;"></a>', unsafe_allow_html=True)
                    else: c7.info("No Link")
                else:
                    c7.write("No Image")
                st.divider()
        else:
            st.success("✅ No CAD delays found with current criteria.")

    elif active_report == "🕒 Ghat Delay Report":
        st.header("🕒 Ghat Delay Report")
        st.info("Logic: Metal Issued but Dia Not Issued. Delay > 5 days (Small <= 5cts) or > 9 days (Big > 5cts).")
        
        ghat_df = df.copy()
        ghat_df['METAL_ISSUE_DT'] = pd.to_datetime(ghat_df[col_issue_dt], dayfirst=True, errors='coerce')
        
        col_dia_issue = next((c for c in df.columns if 'DIA' in c and 'ISSUE' in c and 'DATE' in c and '2ND' not in c), 'DIA_ISSUE_DATE')
        
        mask = (ghat_df['METAL_ISSUE_DT'].notna()) & \
               (ghat_df[col_dia_issue].isna() | (ghat_df[col_dia_issue].astype(str).str.strip() == ""))
        
        ghat_delay = ghat_df[mask].copy()
        today = datetime.now()
        ghat_delay['DELAY_DAYS'] = (today - ghat_delay['METAL_ISSUE_DT']).dt.days
        
        small_p_mask = (ghat_delay[col_dia] <= 5) & (ghat_delay['DELAY_DAYS'] > 5)
        big_p_mask = (ghat_delay[col_dia] > 5) & (ghat_delay['DELAY_DAYS'] > 9)
        final_ghat = ghat_delay[small_p_mask | big_p_mask].sort_values('DELAY_DAYS', ascending=False)

        if not final_ghat.empty:
            st.write("#### 🔍 Filter Results")
            f1, f2, f3, f4 = st.columns(4)
            
            with f1:
                sel_cust = st.multiselect("Filter by Customer", sorted(final_ghat[col_cust].unique()))
            with f2:
                sel_karigar = st.multiselect("Filter by Karigar", sorted(final_ghat['KARIGAR'].astype(str).unique()))
            with f3:
                sel_otype = st.multiselect("Filter by Order Type", sorted(final_ghat[col_order_type].unique()))
            with f4:
                min_d = final_ghat['METAL_ISSUE_DT'].min().date()
                max_d = final_ghat['METAL_ISSUE_DT'].max().date()
                date_range = st.date_input(
                    "Metal Issue Date (DD/MM/YYYY)", 
                    [min_d, max_d],
                    format="DD/MM/YYYY"
                )

            if sel_cust: final_ghat = final_ghat[final_ghat[col_cust].isin(sel_cust)]
            if sel_karigar: final_ghat = final_ghat[final_ghat['KARIGAR'].astype(str).isin(sel_karigar)]
            if sel_otype: final_ghat = final_ghat[final_ghat[col_order_type].isin(sel_otype)]
            if len(date_range) == 2:
                final_ghat = final_ghat[(final_ghat['METAL_ISSUE_DT'].dt.date >= date_range[0]) & (final_ghat['METAL_ISSUE_DT'].dt.date <= date_range[1])]

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
                c5.write(f"🕒 {int(row['DELAY_DAYS'])} Days")
                c6.write(row.get('KARIGAR', '---'))
                
                img_url = row.get('IMAGE_LINK')
                if img_url and str(img_url).strip() not in ["", "---", "None"]:
                    file_id = str(img_url).split("id=")[1].split("&")[0] if "id=" in str(img_url) else (str(img_url).split("d/")[1].split("/")[0] if "d/" in str(img_url) else None)
                    if file_id:
                        thumb = f"https://lh3.googleusercontent.com/u/0/d/{file_id}"
                        c7.markdown(f'<a href="{img_url}" target="_blank"><img src="{thumb}" width="80px" style="border-radius:5px; border:1px solid #4F4F4F;"></a>', unsafe_allow_html=True)
                st.divider()
        else:
            st.success("✅ No Ghat delays detected.")

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
                
                t_bags = sub_data[col_bag].count()
                t_metal = std_round(sub_data[col_metal].sum())
                t_dia = sub_data[col_dia].sum()
                st.markdown(f"**SUBTOTAL:** {t_bags} Bags | {t_metal}g 18kt | {t_dia:,.2f} Dia Cts")
            else:
                st.info(f"No Metal Pending For {o_type.title()} Orders")

    elif active_report == "📋 CSR":
        st.header("📋 Customer Status Report")
        status_seq = {"SEQUENCE": 0, "ENGRAVING/HUID": 1, "IGI": 2, "ON HAND": 3, "FINAL QC": 4, "SETTING QC OK": 5, "SETTING": 6, "GHAT OK": 7, "CASTING": 8, "METAL ISSUED": 9, "METAL PENDING": 10, "HOLD": 12, "CANCEL": 13}
        csr_df = df.copy()
        csr_df['Seq'] = csr_df[col_status].map(status_seq).fillna(99)
        for cust in sorted(csr_df[col_cust].unique()):
            with st.expander(f"👤 CUSTOMER: {cust}"):
                cust_data = csr_df[csr_df[col_cust] == cust]
                summary = cust_data.groupby([col_status, 'Seq']).agg({col_bag: 'count', col_metal: 'sum', col_dia: 'sum'}).reset_index().sort_values('Seq')
                
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

    elif active_report == "🔍 Bag History Report":
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
                        except: return "0.00"
                    return "0.00"

                def get_date_flex(prefix):
                    val = get_val_flex(prefix)
                    if val == "---": return "---"
                    try:
                        dt = pd.to_datetime(val, dayfirst=True, errors='coerce')
                        return dt.strftime('%d/%m/%Y %I:%M %p') if pd.notnull(dt) else str(val)
                    except: return str(val)

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

    elif menu == "💰 Sales Analytics":
        st.header("💎 Sales Analytics")
        sdf = fetch_sales_data()
        
        if sdf is not None:
            try:
                import plotly.express as px
                
                s_report = pd.DataFrame({
                    'Customer': sdf.iloc[:, 0].astype(str).str.strip(),
                    'Karigar': sdf.iloc[:, 9].astype(str).str.strip(),
                    'Dia_Cts': pd.to_numeric(sdf.iloc[:, 11], errors='coerce').fillna(0),
                    'Date': pd.to_datetime(sdf.iloc[:, 19], dayfirst=True, errors='coerce')
                })

                s_report = s_report.dropna(subset=['Date'])
                s_report = s_report[s_report['Date'].dt.year == 2026]
                s_report = s_report[~s_report['Customer'].isin(["None", "nan", ""])]

                if not s_report.empty:
                    s_report['Month'] = s_report['Date'].dt.strftime('%B')
                    month_order = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
                    
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
                        template="plotly_dark",
                        animation_frame=None
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
