import streamlit as st
import pandas as pd
import requests
import numpy as np

# 1. SETUP
st.set_page_config(page_title="MASTER DATABASE", layout="wide", initial_sidebar_state="collapsed")

# --- DATA SOURCE LINKS ---
# Master inventory data from Google Sheets (via Google Apps Script API)
MASTER_API_URL = "https://script.google.com/macros/s/AKfycbzJeiT_mTmPFVEFDqDZvnZeakdFVxUrGiOjtl-NBgGFHyi3HYLCO1648JSm7s2bW0A/exec"

# URLs for movement history spreadsheets (CSV output)
LIVE_CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRk0Sr33sugG2FtNtdqqk11u8b2aGKGniTN1n2qcWiOCg1W0Vi5JzqLWZdu1DWVUfpP2baURyOn4qo7/pub?gid=449683950&single=true&output=csv"
ARCHIVE_CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRk0Sr33sugG2FtNtdqqk11u8b2aGKGniTN1n2qcWiOCg1W0Vi5JzqLWZdu1DWVUfpP2baURyOn4qo7/pub?gid=1700828894&single=true&output=csv"
POST_1 = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRk0Sr33sugG2FtNtdqqk11u8b2aGKGniTN1n2qcWiOCg1W0Vi5JzqLWZdu1DWVUfpP2baURyOn4qo7/pub?gid=1461249957&single=true&output=csv"
POST_2 = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRk0Sr33sugG2FtNtdqqk11u8b2aGKGniTN1n2qcWiOCg1W0Vi5JzqLWZdu1DWVUfpP2baURyOn4qo7/pub?gid=163646832&single=true&output=csv"

# Custom CSS for Minimalist UI, Dark Header Sections, and Red "X" for missing data
st.markdown("""
    <style>
    [data-testid="stMetric"] { background-color: rgba(125, 125, 125, 0.1); padding: 10px; border-radius: 8px; }
    .stTable { overflow-x: auto; }
    .section-head { background-color: #2e2e2e; color: #f0f0f0; padding: 8px 15px; border-radius: 5px; margin: 20px 0 10px 0; font-weight: bold; }
    .missing-data { color: #ff4b4b; font-weight: bold; }
    </style>
    """, unsafe_allow_html=True)

def check_password():
    """Simple password protection for the app."""
    if "password_correct" not in st.session_state:
        st.title("🔒 Master Structure & Reports")
        pwd = st.text_input("Workshop Password", type="password")
        if st.button("Login"):
            # Minimalistic password check
            if pwd == "12345": 
                st.session_state["password_correct"] = True
                st.rerun()
            else: st.error("Invalid Password")
        return False
    return True

if check_password():
    
    @st.cache_data(ttl=60)
    def fetch_master():
        """Fetches the master inventory data and cleans it."""
        try:
            r = requests.get(MASTER_API_URL, timeout=15)
            # Replace empty strings with NaN for consistency, drop rows with missing Bag No
            return pd.DataFrame(r.json()).replace('', np.nan).dropna(subset=['Bag_No'], how='all')
        except Exception as e: 
            st.error(f"Error fetching Master Data: {e}")
            return None

    @st.cache_data(ttl=60)
    def fetch_history(urls):
        """Fetches movement history from multiple CSV URLs and combines them."""
        try:
            frames = []
            for url in urls:
                # The sheets have a single row above the header
                tdf = pd.read_csv(url, skiprows=1)
                # Clean whitespace from column names
                tdf.columns = tdf.columns.str.strip()
                frames.append(tdf)
            combined = pd.concat(frames, ignore_index=True)
            # Ensure BAG NO is treated as a clean string for strict matching
            combined['BAG NO'] = combined['BAG NO'].astype(str).str.strip()
            return combined
        except Exception as e: 
            st.error(f"Error fetching History Data: {e}")
            return None

    # --- Load Data from Google Sheets ---
    df = fetch_master()
    # PRE-FINISH: Combines current workshop movements and older archived records
    df_pre = fetch_history([LIVE_CSV_URL, ARCHIVE_CSV_URL])
    # POST-FINISH: Combines movements from subsequent production stages
    df_post = fetch_history([POST_1, POST_2])

    # Helper Functions
    def get_val(val):
        """Displays a value or a red 'X' if data is missing or pending."""
        if pd.isna(val) or str(val).strip().lower() in ['nan', '', 'none', 'pending']:
            return '<span class="missing-data">X</span>'
        return str(val)

    def std_round(x):
        """Rounds a number to the nearest integer for metal calculations."""
        try: return int(float(x) + 0.5) if float(x) > 0 else 0
        except: return 0

    def show_clean_tables(hist_df, label, search_bag):
        """Displays Inward and Outward movement logs for a given bag number."""
        st.markdown(f'<div class="section-head">{label} MOVEMENT</div>', unsafe_allow_html=True)
        if hist_df is not None:
            # Strictly match the bag number after cleaning spaces
            search_bag_str = str(search_bag).strip()
            moves = hist_df[hist_df['BAG NO'].astype(str).str.strip() == search_bag_str].copy()
            
            if not moves.empty:
                # Normalize column names for flexible matching against sheet inconsistencies
                moves.columns = moves.columns.str.upper().str.strip()
                
                h_in, h_out = st.columns(2)
                with h_in:
                    st.info("Inward")
                    # Flexible search for the Date and Purpose columns for Inward movements
                    date_in_col = next((c for c in moves.columns if 'INWARD' in c and 'DATE' in c), None)
                    # FIX: Looks for *any* column containing both 'PURPOSE' and 'IN' (like 'IN PURPOSE' or 'PURPOSE IN')
                    purp_in_col = next((c for c in moves.columns if 'PURPOSE' in c and 'IN' in c), None)
                    
                    if date_in_col and purp_in_col:
                        in_df = moves[[date_in_col, purp_in_col]].copy()
                        # Replace potential missing purpose values with N/A instead of dropping the row
                        in_df[purp_in_col] = in_df[purp_in_col].fillna("N/A")
                        # Only show movements that have at least a DATE recorded
                        in_df = in_df.dropna(subset=[date_in_col])
                        st.table(in_df.rename(columns={date_in_col: 'Date', purp_in_col: 'Purpose'}))
                    else: st.warning("Inward columns not found in sheet structure.")
                    
                with h_out:
                    st.error("Outward")
                    # Flexible search for the Date and Purpose columns for Outward movements
                    date_out_col = next((c for c in moves.columns if 'OUTWARD' in c and 'DATE' in c), None)
                    purp_out_col = next((c for c in moves.columns if 'PURPOSE' in c and 'OUT' in c), None)
                    
                    if date_out_col and purp_out_col:
                        out_df = moves[[date_out_col, purp_out_col]].copy()
                        out_df[purp_out_col] = out_df[purp_out_col].fillna("N/A")
                        out_df = out_df.dropna(subset=[date_out_col])
                        st.table(out_df.rename(columns={date_out_col: 'Date', purp_out_col: 'Purpose'}))
                    else: st.warning("Outward columns not found in sheet structure.")
            else: 
                st.info(f"No {label} logs found for this bag.")

    st.sidebar.title("💼 ADMIN")
    report_choice = st.sidebar.selectbox("GO TO:", ["🏠 Home", "🔍 Bag History Report", "📊 Metal Requirements", "📋 CSR"])
    
    if st.sidebar.button("Logout"):
        del st.session_state["password_correct"]
        st.rerun()

    if df is not None:
        
        # --- 1. HOME / SEARCH ---
        if report_choice == "🏠 Home":
            st.subheader("🔍 Search Inventory")
            search = st.text_input("Search Style/Bag...", placeholder="Type here...")
            display_df = df.copy()
            if search:
                # Basic full-text search across Style No and Bag No columns
                display_df = display_df[display_df['Style_No'].astype(str).str.contains(search, case=False) | 
                                        display_df['Bag_No'].astype(str).str.contains(search, case=False)]
            st.dataframe(display_df, column_config={"Thumbnail_Link": st.column_config.ImageColumn("Preview")}, use_container_width=True, hide_index=True)

        # --- 2. BAG HISTORY REPORT ---
        elif report_choice == "🔍 Bag History Report":
            st.subheader("🔍 Bag History Report")
            search_bag = st.text_input("Search Bag Number").strip()
            
            if search_bag:
                # Find the primary bag data in the Master Database
                m_data = df[df['Bag_No'].astype(str) == search_bag]
                if not m_data.empty:
                    row = m_data.iloc[0]
                    st.markdown('<div class="section-head">Bag Details</div>', unsafe_allow_html=True)
                    
                    c1, c2 = st.columns([2, 1])
                    c1.markdown(f"### Bag NO: `{search_bag}`")
                    status = row.get('Final_VZ_Status', 'N/A')
                    c2.warning(f"**Status:** {status}")
                    
                    colA, colB = st.columns(2)
                    with colA:
                        st.write(f"1. **Customer:** {get_val(row.get('Customer'))}", unsafe_allow_html=True)
                        st.write(f"2. **Type:** {get_val(row.get('Order_Type'))}", unsafe_allow_html=True)
                        st.write(f"3. **Order Date:** {get_val(row.get('Order_Date'))}", unsafe_allow_html=True)
                        st.write(f"4. **Karigar:** {get_val(row.get('Karigar'))}", unsafe_allow_html=True)
                    with colB:
                        st.write(f"5. **Metal:** {get_val(row.get('Metal'))}g", unsafe_allow_html=True)
                        st.write(f"6. **Dia Cts:** {get_val(row.get('Dia_Cts'))}", unsafe_allow_html=True)
                        
                        m_issue = row.get('Metal_Issue_Date')
                        # Hardcoded logic example for a specific business rule: 
                        # Assume issue date is 25/03/2026 if status is CASTING but date is missing in sheet.
                        if (pd.isna(m_issue) or str(m_issue).strip() == "") and status == "CASTING":
                            m_issue = "25/03/2026"
                        
                        st.write(f"7. **Metal Issue:** {get_val(m_issue)}", unsafe_allow_html=True)
                        st.write(f"8. **Deliv. Date:** {get_val(row.get('Delivery_Date'))}", unsafe_allow_html=True)

                    # Display movement history from both PRE and POST datasets
                    show_clean_tables(df_pre, "PRE-FINISH", search_bag)
                    show_clean_tables(df_post, "POST-FINISH", search_bag)
                else: st.error("Bag number not found in Master Database.")

        # --- 3. METAL REQUIREMENTS ---
        elif report_choice == "📊 Metal Requirements":
            st.subheader("📊 Metal Requirements")
            # Convert Metal to numeric, filling missing values with 0
            df['Metal'] = pd.to_numeric(df['Metal'], errors='coerce').fillna(0)
            # Filter for orders that have no issue date (pending) or explicitly marked as PENDING status
            p_df = df[(df['Metal_Issue_Date'].isna()) | (df['Final_VZ_Status'] == "METAL PENDING")].copy()
            
            def create_metal_card(data, label):
                # Aggregate data by Customer to show quantity and total gold weight
                summary = data.groupby('Customer').agg({'Bag_No': 'count', 'Metal': 'sum'})
                summary['Metal 18kt'] = summary['Metal'].apply(std_round)
                # Formula example: Pure gold weight is approx 76% of 18kt gold
                summary['Pure'] = (summary['Metal 18kt'] * 0.76).apply(std_round)
                c1, c2 = st.columns(2)
                c1.metric(f"{label} Bags", summary['Bag_No'].sum())
                c2.metric(f"18kt Total", f"{summary['Metal 18kt'].sum()}g")
                st.table(summary[['Bag_No', 'Metal 18kt', 'Pure']].rename(columns={'Bag_No': 'Qty'}))

            # Display separate summaries for Customer orders vs. Stock orders
            st.info("👤 CUSTOMER ORDERS")
            c_df = p_df[p_df['Order_Type'].str.contains('CUSTOMER', case=False, na=False)]
            if not c_df.empty: create_metal_card(c_df, "Cust")
            
            st.warning("📦 STOCK ORDERS")
            s_df = p_df[p_df['Order_Type'].str.contains('STOCK', case=False, na=False)]
            if not s_df.empty: create_metal_card(s_df, "Stock")

        # --- 4. CSR (CUSTOMER STATUS REPORT) ---
        elif report_choice == "📋 CSR":
            st.subheader("📋 Customer Status Report")
            # Custom sorting order for production statuses from final to beginning
            status_seq = {"SEQUENCE": 0, "ENGRAVING/HUID": 1, "IGI": 2, "ON HAND": 3, "FINAL QC": 4, "SETTING QC OK": 5, "SETTING": 6, "GHAT OK": 7, "CASTING": 8, "METAL ISSUED": 9, "METAL PENDING": 10}
            csr_df = df.copy()
            # Prepare data: convert numbers to numeric, map statuses to sequence number
            csr_df['Metal'] = pd.to_numeric(csr_df['Metal'], errors='coerce').fillna(0)
            csr_df['Dia_Cts'] = pd.to_numeric(csr_df['Dia_Cts'], errors='coerce').fillna(0)
            csr_df['Seq'] = csr_df['Final_VZ_Status'].map(status_seq).fillna(99)

            # Create an expandable section for each customer, sorted alphabetically
            for cust in sorted(csr_df['Customer'].unique()):
                with st.expander(f"👤 {cust}"):
                    cust_data = csr_df[csr_df['Customer'] == cust]
                    # Aggregate quantity, metal, and diamond cts per status, sorted by production sequence
                    summary = cust_data.groupby(['Final_VZ_Status', 'Seq']).agg({'Bag_No': 'count', 'Metal': 'sum', 'Dia_Cts': 'sum'}).reset_index().sort_values('Seq')
                    summary['Metal 18kt'] = summary['Metal'].apply(std_round)
                    summary['Dia Cts'] = summary['Dia_Cts'].map('{:,.2f}'.format)
                    st.table(summary[['Final_VZ_Status', 'Bag_No', 'Metal 18kt', 'Dia Cts']].rename(columns={'Final_VZ_Status': 'Status', 'Bag_No': 'Qty'}))
                    # Show grand total summary for the entire customer
                    st.markdown(f"**TOTAL:** `{summary['Bag_No'].sum()}` Bags | `{summary['Metal 18kt'].sum()}g` 18kt | `{summary['Dia_Cts'].sum():.2f}` Dia Cts")
