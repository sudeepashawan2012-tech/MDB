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

        # These scopes are required to let BigQuery "talk" to your Google Sheets

        scopes = ["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/drive"]

        creds = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)

        client = bigquery.Client(credentials=creds, project=creds.project_id)

        

        query = "SELECT * FROM `jewelry-sql-system.workshop_data.master_inventory`"

        df = client.query(query).to_dataframe()

        

        # Standardize Master headers

        df.columns = [str(c).strip().upper().replace(' ', '_').replace('.', '_').replace('/', '_') for c in df.columns]

        

        # Remove blank rows

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

    """Formats SQL dates to DD-Mon-YYYY (e.g., 14-Apr-2026)"""

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

        # Dynamic Column Mapping for main reports

        col_metal = next((c for c in df.columns if 'METAL' in c and '18' in c and 'WT' in c), 'METAL_18KT_WT')

        col_status = next((c for c in df.columns if 'STATUS' in c and 'DATE' not in c), 'CURRENT_STATUS')

        col_cust = next((c for c in df.columns if 'CUSTOMER' in c), 'CUSTOMER')

        col_order_type = next((c for c in df.columns if 'ORDER_TYPE' in c), 'ORDER_TYPE')

        col_bag = next((c for c in df.columns if 'BAG' in c), 'BAG_NO')

        col_dia = next((c for c in df.columns if 'DIA' in c and 'CTS' in c), 'DIA_CTS')

        col_issue_dt = next((c for c in df.columns if 'METAL' in c and 'ISSUE' in c and 'DATE' in c), 'METAL_ISSUE_DATE')



        df[col_metal] = pd.to_numeric(df[col_metal], errors='coerce').fillna(0)

        df[col_dia] = pd.to_numeric(df[col_dia], errors='coerce').fillna(0)



        menu = st.sidebar.radio("SELECT REPORT", ["📊 Metal Requirements", "📋 CSR", "🔍 Bag History Report"])



        # --- REPORT 1: METAL REQUIREMENTS (UNCHANGED) ---

        if menu == "📊 Metal Requirements":

            st.header("📊 Metal Requirement Report")

            exclude = ["HOLD", "CANCEL"]

            mask = (df[col_issue_dt].isna() | (df[col_issue_dt].astype(str).str.strip() == "")) & (~df[col_status].isin(exclude))

            pending_df = df[mask].copy()



            for o_type in ["CUSTOMER", "STOCK"]:

                st.subheader(f"📍 {o_type} ORDERS")

                sub_data = pending_df[pending_df[col_order_type].str.contains(o_type.split()[0], case=False, na=False)]

                

                if not sub_data.empty:

                    summary = sub_data.groupby(col_cust).agg({

                        col_bag: 'count',

                        col_metal: 'sum',

                        col_dia: 'sum'

                    }).reset_index()

                    

                    summary.columns = ['Customer Code', 'Bag Qty', 'Metal 18kt', 'Dia Cts']

                    summary['Metal 18kt'] = summary['Metal 18kt'].apply(std_round)

                    summary['Dia Cts'] = summary['Dia Cts'].map('{:,.2f}'.format)

                    st.table(summary)

                    

                    t_bags = sub_data[col_bag].count()

                    t_metal = std_round(sub_data[col_metal].sum())

                    t_dia = sub_data[col_dia].sum()

                    st.markdown(f"""<div style="font-size:22px; font-weight:bold; border-top:2px solid #eee; padding-top:10px;">

                        SUBTOTAL: {t_bags} Bags | {t_metal}g 18kt | {t_dia:,.2f} Dia Cts

                        </div>""", unsafe_allow_html=True)

                else:

                    st.info(f"No Metal Pending For {o_type.title()} Orders")



        # --- REPORT 2: CSR (UNCHANGED) ---

        elif menu == "📋 CSR":

            st.header("📋 Customer Status Report")

            status_seq = {

                "SEQUENCE": 0, "ENGRAVING/HUID": 1, "IGI": 2, "ON HAND": 3, 

                "FINAL QC": 4, "SETTING QC OK": 5, "SETTING": 6, "GHAT OK": 7, 

                "CASTING": 8, "METAL ISSUED": 9, "METAL PENDING": 10, 

                "HOLD": 12, "CANCEL": 13

            }

            

            csr_df = df.copy()

            csr_df['Seq'] = csr_df[col_status].map(status_seq).fillna(99)

            customers = sorted(csr_df[col_cust].unique())

            

            for cust in customers:

                with st.expander(f"👤 CUSTOMER: {cust}"):

                    cust_data = csr_df[csr_df[col_cust] == cust]

                    summary = cust_data.groupby([col_status, 'Seq']).agg({

                        col_bag: 'count',

                        col_metal: 'sum',

                        col_dia: 'sum'

                    }).reset_index().sort_values('Seq')

                    

                    summary['Metal 18kt'] = summary[col_metal].apply(std_round)

                    summary['Dia Cts'] = summary[col_dia].map('{:,.2f}'.format)

                    

                    display_tab = summary[[col_status, col_bag, 'Metal 18kt', 'Dia Cts']].rename(

                        columns={col_status: 'Status', col_bag: 'Bag Qty'}

                    )

                    st.dataframe(display_tab, hide_index=True, use_container_width=True)

                    

                    t_cust_bags = summary[col_bag].sum()

                    t_cust_metal = std_round(summary[col_metal].sum())

                    t_cust_dia = summary[col_dia].sum()

                    st.markdown(f"""<div style="font-size:20px; font-weight:bold; border-top:1px solid #ccc; padding-top:5px; color:#1f77b4;">

                        TOTAL: {t_cust_bags} Bags | {t_cust_metal}g 18kt | {t_cust_dia:,.2f} Dia Cts

                        </div>""", unsafe_allow_html=True)



       # --- REPORT 3: BAG HISTORY (QC PROCESS ADDED) ---

        elif menu == "🔍 Bag History Report":

            st.header("🔍 Bag History Report")

            search_bag = st.text_input("Enter Bag Number to Search").strip()

            

            if search_bag:

                match = df[df[col_bag].astype(str).str.upper() == search_bag.upper()]

                

                if not match.empty:

                    r = match.iloc[0]

                    

                    # SECTION 1: MASTER DETAILS

                    st.markdown("### 📦 Bag Master Details")

                    mc1, mc2 = st.columns(2)

                    with mc1:

                        st.write(f"**Customer:** {r.get(col_cust, 'N/A')}")

                        st.write(f"**Order Type:** {r.get(col_order_type, 'N/A')}")

                        st.write(f"**Karigar:** {r.get('KARIGAR', 'N/A')}")

                        st.write(f"**Metal:** {std_round(r.get(col_metal, 0))}g 18kt")

                        st.write(f"**Dia Cts:** {float(r.get(col_dia, 0)):.2f}")

                    with mc2:

                        st.write(f"**Order Date:** {clean_date(r.get('ORDER_DATE'))}")

                        st.write(f"**Metal Issue:** {clean_date(r.get(col_issue_dt))}")

                        dia_dt = r.get('DIA_ISSUE_DATE', r.get('DIAMOND_DATE', r.get('DIAMOND_ISSUE_DATE', '---')))

                        st.write(f"**Dia Issue:** {clean_date(dia_dt)}") 

                        st.write(f"**Delivery Date:** {clean_date(r.get('DELIVERY_DATE'))}")

                        st.write(f"**Current Status:** {r.get(col_status, 'N/A')}")



                    st.divider()



                    # SECTION 2: QC PROCESS REPORT (SMART MAPPING)

                    st.markdown("### 📋 QC Process Report")

                    

                    # This helper looks for the column letter even if it has underscores or spaces

                    def find_col(letter):

                        potential_names = [letter, f"_{letter}_", f"COLUMN_{letter}", letter.upper()]

                        for name in potential_names:

                            if name in match.columns:

                                return name

                        return None



                    def get_smart_val(letter, default="---"):

                        col = find_col(letter)

                        if col and pd.notna(r[col]):

                            return r[col]

                        return default



                    q1, q2, q3 = st.columns(3)

                    

                    with q1:

                        st.markdown("**🛠️ GHAT DETAILS**")

                        st.write(f"QC: {get_smart_val('X')}")

                        st.write(f"Weight: {get_smart_val('Y', '0')}g")

                        # Falling back to GHAT_DATE if it exists in your master df

                        st.write(f"Date: {clean_date(r.get('GHAT_DATE', '---'))}")

                    

                    with q2:

                        st.markdown("**💎 SETTING DETAILS**")

                        st.write(f"QC: {get_smart_val('AH')}")

                        st.write(f"Weight: {get_smart_val('AY', '0')}g")

                        st.write(f"Date: {clean_date(r.get('SETTING_DATE', '---'))}")



                    with q3:

                        st.markdown("**✨ FINAL FINISH**")

                        st.write(f"Final QC: {get_smart_val('AK')}")

                        st.write(f"Final Wt: {get_smart_val('AL', '0')}g")

                        st.write(f"QC Date: {clean_date(get_smart_val('AM'))}")

                        st.write(f"Finish Date: {clean_date(r.get('FINISH_DATE', '---'))}")



                    st.markdown("---")

                    st.markdown("**🎨 COLOURSTONE DETAILS**")

                    cs1, cs2 = st.columns(2)

                    with cs1:

                        st.caption("1st Issue")

                        st.write(f"Person: {get_smart_val('AB')}")

                        st.write(f"Qty: {get_smart_val('AC', '0')}")

                        st.write(f"Date: {clean_date(get_smart_val('AD'))}")

                    with cs2:

                        st.caption("2nd Issue")

                        st.write(f"Person: {get_smart_val('AE')}")

                        st.write(f"Qty: {get_smart_val('AF', '0')}")

                        st.write(f"Date: {clean_date(get_smart_val('AG'))}")



                    st.divider()

                    

                    # SECTION 3: MOVEMENT DATA (PRE & POST)

                    try:

                        scopes = ["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/drive"]

                        creds = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)

                        client = bigquery.Client(credentials=creds, project=creds.project_id)

                        

                        def get_movement_data(table_id):

                            query = f"SELECT * FROM `jewelry-sql-system.workshop_data.{table_id}` WHERE BAG_NO = '{search_bag}'"

                            m_df = client.query(query).to_dataframe()

                            if m_df.empty: return m_df

                            m_df.columns = [str(c).strip().upper().replace(' ', '_').replace('.', '_') for c in m_df.columns]

                            for c in m_df.columns:

                                if 'DATE' in c:

                                    m_df[c] = pd.to_datetime(m_df[c], errors='coerce').dt.strftime('%d/%m/%Y')

                            return m_df



                        st.markdown("### 🛠️ PRE-FINISH MOVEMENT")

                        df_pre = get_movement_data("pre_finish_movement")

                        c1, c2 = st.columns(2)

                        with c1:

                            st.markdown('<p style="background-color:#E8F0FE; padding:8px; border-radius:5px; color:black; font-weight:bold;">Inward</p>', unsafe_allow_html=True)

                            if not df_pre.empty:

                                cols = [c for c in df_pre.columns if ('IN' in c or 'PURPOSE' in c) and 'OUT' not in c and 'BAG' not in c]

                                st.dataframe(df_pre[cols].dropna(how='all'), hide_index=True, use_container_width=True)

                        with c2:

                            st.markdown('<p style="background-color:#FEE8E8; padding:8px; border-radius:5px; color:black; font-weight:bold;">Outward</p>', unsafe_allow_html=True)

                            if not df_pre.empty:

                                cols = [c for c in df_pre.columns if 'OUT' in c and 'BAG' not in c]

                                st.dataframe(df_pre[cols].dropna(how='all'), hide_index=True, use_container_width=True)



                        st.write("") 



                        st.markdown("### ✨ POST-FINISH MOVEMENT")

                        df_post = get_movement_data("post_finish_movement")

                        c3, c4 = st.columns(2)

                        with c3:

                            st.markdown('<p style="background-color:#FEE8E8; padding:8px; border-radius:5px; color:black; font-weight:bold;">Outward</p>', unsafe_allow_html=True)

                            if not df_post.empty:

                                cols = [c for c in df_post.columns if 'OUT' in c and 'BAG' not in c]

                                st.dataframe(df_post[cols].dropna(how='all'), hide_index=True, use_container_width=True)

                        with c4:

                            st.markdown('<p style="background-color:#E8F0FE; padding:8px; border-radius:5px; color:black; font-weight:bold;">Inward</p>', unsafe_allow_html=True)

                            if not df_post.empty:

                                cols = [c for c in df_post.columns if ('IN' in c or 'PURPOSE' in c) and 'OUT' not in c and 'BAG' not in c]

                                st.dataframe(df_post[cols].dropna(how='all'), hide_index=True, use_container_width=True)



                    except Exception as mv_e:

                        st.error(f"Data Error: {mv_e}")

                else:

                    st.warning(f"Bag No {search_bag} not found.")
                    # --- NEW REPORT: SCOPE OF WORK ---
        elif menu == "📋 Scope of Work":
            st.header("📋 Scope of Work")
            
            # 1. DATA PREPARATION
            issued_mask = df[col_issue_dt].notna() & (df[col_issue_dt].astype(str).str.strip() != "")
            is_cust = df[col_order_type].str.contains("CUSTOMER", case=False, na=False)
            is_stock = df[col_order_type].str.contains("STOCK", case=False, na=False)
            
            # Helper for consistent table formatting with a TOTAL row
            def get_report_table(data):
                if data.empty:
                    return None
                
                # Group data by Customer
                grp = data.groupby(col_cust).agg({
                    col_bag: 'count',
                    col_metal: 'sum',
                    col_dia: 'sum'
                }).reset_index()
                grp.columns = ['Customer Name', 'Ord Qty', 'Metal 18kt', 'Dia Cts']
                
                # Calculate Totals for the bottom row
                total_qty = grp['Ord Qty'].sum()
                total_metal = grp['Metal 18kt'].sum()
                total_dia = grp['Dia Cts'].sum()
                
                # Create the total row
                total_row = pd.DataFrame([{
                    'Customer Name': 'TOTAL',
                    'Ord Qty': total_qty,
                    'Metal 18kt': total_metal,
                    'Dia Cts': total_dia
                }])
                
                # Append total row to the dataframe
                final_df = pd.concat([grp, total_row], ignore_index=True)
                
                # Final Formatting
                final_df['Metal 18kt'] = final_df['Metal 18kt'].apply(std_round)
                final_df['Dia Cts'] = final_df['Dia Cts'].map('{:,.2f}'.format)
                
                return final_df

            def display_section(title, data):
                st.markdown(f"### {title}")
                table = get_report_table(data)
                if table is not None:
                    # Styling the total row to stand out (Optional: using st.table)
                    st.table(table)
                else:
                    st.info(f"No data available for {title}")
                st.divider()

            # --- 2. TOTAL SCOPE OF WORK (GRAND TOTAL) ---
            st.subheader("📊 Total Scope of Work")
            gt_bags = df[col_bag].count()
            gt_metal = std_round(df[col_metal].sum())
            gt_dia = df[col_dia].sum()
            
            st.markdown(f"""
                <div style="background-color:#1E1E1E; padding:25px; border-radius:10px; border:2px solid #4F4F4F; text-align:center; color: white;">
                    <div style="font-size:16px; letter-spacing: 2px; color: #BBBBBB;">GRAND TOTAL</div>
                    <div style="font-size:28px; font-weight:bold; margin-top:10px;">
                        {gt_bags} Ord Qty &nbsp; | &nbsp; {gt_metal} Metal 18kt &nbsp; | &nbsp; {gt_dia:,.2f} Dia Cts
                    </div>
                </div>
            """, unsafe_allow_html=True)
            st.write("") 

            # --- 3. THE TWO MAIN SUMMARIES ---
            display_section("Customer Orders", df[is_cust])
            display_section("Stock Orders", df[is_stock])
            
            # --- 4. THE FOUR-WAY BREAKDOWN ---
            st.subheader("🔍 Detailed Breakdown (Issued vs Pending)")
            
            display_section("Metal Issued Customer Orders", df[issued_mask & is_cust])
            display_section("Metal Pending Customer Orders", df[~issued_mask & is_cust])
            display_section("Metal Issued Stock Orders", df[issued_mask & is_stock])
            display_section("Metal Pending Stock Orders", df[~issued_mask & is_stock])
