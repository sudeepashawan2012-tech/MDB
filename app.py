import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# PAGE CONFIG
st.set_page_config(page_title="MASTER DATABASE", layout="wide", initial_sidebar_state="collapsed")

# --- SQL FETCH ---
@st.cache_data(ttl=300)
def fetch_master_from_sql():
    try:
        scopes = ["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/drive"]
        creds = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
        client = bigquery.Client(credentials=creds, project=creds.project_id)
        query = "SELECT * FROM `jewelry-sql-system.workshop_data.master_inventory`"
        df = client.query(query).to_dataframe()
        # Cleaning column names: Strip spaces, convert to upper, replace internal spaces with underscore
        df.columns = [c.strip().upper().replace(' ', '_').replace('.', '').replace('/', '') for c in df.columns] 
        return df
    except Exception as e:
        st.error(f"SQL Error: {e}")
        return None

# --- MOVEMENT FETCH (Merging Current + Old) ---
@st.cache_data(ttl=300)
def fetch_movement(urls):
    try:
        frames = []
        for url in urls:
            # We read only relevant columns: Date, Time, Process, Bag No, Status
            tmp = pd.read_csv(url, skiprows=1, header=None)
            frames.append(tmp)
        return pd.concat(frames, ignore_index=True)
    except:
        return None

# --- HELPER: BOLD RED X ---
def get_val(val, is_date=False):
    if pd.isna(val) or str(val).strip().lower() in ['nan', '', 'none', 'pending', 'false']:
        return '<span style="color: #ff4b4b; font-weight: bold;">X</span>'
    if is_date:
        try: return pd.to_datetime(val).strftime('%d/%m/%Y')
        except: return str(val)
    return str(val)

# --- AUTH ---
if "password_correct" not in st.session_state:
    st.title("🔒 Master Structure & Reports")
    pwd = st.text_input("Workshop Password", type="password")
    if st.button("Login"):
        if pwd == st.secrets["workshop_password"]: 
            st.session_state["password_correct"] = True
            st.rerun()
else:
    df = fetch_master_from_sql()
    
    # MOVEMENT LINKS
    PRE_URLS = [
        "https://docs.google.com/spreadsheets/d/e/2PACX-1vRRFxP8TXKiWTs84_GYxL_q44od1bnk0hcI6bFXFaTK1oBpfe4PbDbT3pAn3-zKGq1KVDngzIfvdmmv/pub?gid=1569208336&single=true&output=csv", # Current
        "https://docs.google.com/spreadsheets/d/e/2PACX-1vRk0Sr33sugG2FtNtdqqk11u8b2aGKGniTN1n2qcWiOCg1W0Vi5JzqLWZdu1DWVUfpP2baURyOn4qo7/pub?gid=1700828894&single=true&output=csv"  # Old
    ]
    POST_URLS = [
        "https://docs.google.com/spreadsheets/d/e/2PACX-1vRk0Sr33sugG2FtNtdqqk11u8b2aGKGniTN1n2qcWiOCg1W0Vi5JzqLWZdu1DWVUfpP2baURyOn4qo7/pub?gid=1461249957&single=true&output=csv", # Current
        "https://docs.google.com/spreadsheets/d/e/2PACX-1vRk0Sr33sugG2FtNtdqqk11u8b2aGKGniTN1n2qcWiOCg1W0Vi5JzqLWZdu1DWVUfpP2baURyOn4qo7/pub?gid=163646832&single=true&output=csv"  # Old
    ]

    df_pre = fetch_movement(PRE_URLS)
    df_post = fetch_movement(POST_URLS)

    # SIDEBAR
    report_choice = st.sidebar.selectbox("GO TO:", ["🏠 Home", "🔍 Bag History Report", "📊 Metal Requirements", "📋 CSR"])

    if df is not None:
        if report_choice == "🔍 Bag History Report":
            st.subheader("🔍 Bag History Report")
            search_bag = st.text_input("Search Bag Number").strip()
            
            if search_bag:
                m_data = df[df['BAG_NO'].astype(str) == search_bag]
                if not m_data.empty:
                    row = m_data.iloc[0]
                    
                    # 1. TOP SECTION (CORRECT AS PER PIC 1)
                    st.markdown(f"### Bag No: `{search_bag}` | Current Status: `{row.get('CURRENT_STATUS', 'X')}`")
                    st.divider()
                    colA, colB = st.columns(2)
                    with colA:
                        st.markdown(f"1. **Customer:** {get_val(row.get('CUSTOMER'))}", unsafe_allow_html=True)
                        st.markdown(f"2. **Karigar:** {get_val(row.get('KARIGAR'))}", unsafe_allow_html=True)
                        st.markdown(f"3. **Metal Wt 18kt:** {get_val(row.get('METAL_18KT_WT'))}", unsafe_allow_html=True)
                        st.markdown(f"4. **Dia Cts:** {get_val(row.get('DIA_CTS'))}", unsafe_allow_html=True)
                    with colB:
                        st.markdown(f"**Metal Issue Dt:** {get_val(row.get('METAL_ISSUE_DATE'), True)}", unsafe_allow_html=True)
                        st.markdown(f"**Dia Issue Dt:** {get_val(row.get('DIA_ISSUE_DATE'), True)}", unsafe_allow_html=True)
                        st.markdown(f"**Finish Date:** {get_val(row.get('FINISH_DATE'), True)}", unsafe_allow_html=True)
                        st.markdown(f"**IGI Date:** {get_val(row.get('IGI_DATE'), True)}", unsafe_allow_html=True)

                    # 2. PRODUCTION STAGES (MAPPED TO YOUR COLUMN IDs)
                    st.divider()
                    
                    # GHAT QC (X, Y, AA)
                    st.markdown("#### GHAT QC")
                    g1, g2, g3 = st.columns(3)
                    g1.write(f"**Person Name:** {get_val(row.get('GHAT_QC'))}", unsafe_allow_html=True)
                    g2.write(f"**Ghat Wt:** {get_val(row.get('GHAT_WT'))}", unsafe_allow_html=True)
                    g3.write(f"**Ghat Date:** {get_val(row.get('GHAT_DATE'), True)}", unsafe_allow_html=True)
                    
                    # COLOURSTONE (AB, AC, AD and AE, AF, AG)
                    st.markdown("#### COLOURSTONE")
                    cs1, cs2 = st.columns(2)
                    with cs1:
                        st.write(f"**1st Issuer:** {get_val(row.get('CS_1ST_ISSUER'))}", unsafe_allow_html=True)
                        st.write(f"**Qty:** {get_val(row.get('CS_1ST_ISSUE_QTY'))}", unsafe_allow_html=True)
                        st.write(f"**Date:** {get_val(row.get('CS_1ST_ISSUE_DATE'), True)}", unsafe_allow_html=True)
                    with cs2:
                        st.write(f"**2nd Issuer:** {get_val(row.get('CS_2ND_ISSUER'))}", unsafe_allow_html=True)
                        st.write(f"**Qty:** {get_val(row.get('CS_2ND_ISSUE_QTY'))}", unsafe_allow_html=True)
                        st.write(f"**Date:** {get_val(row.get('CS_2ND_ISSUE_DATE'), True)}", unsafe_allow_html=True)

                    # SETTING QC (AH, AI, AJ)
                    st.markdown("#### SETTING QC")
                    s1, s2, s3 = st.columns(3)
                    s1.write(f"**Person Name:** {get_val(row.get('SETTING_QC'))}", unsafe_allow_html=True)
                    s2.write(f"**Setting Wt:** {get_val(row.get('SETTING_WT'))}", unsafe_allow_html=True)
                    s3.write(f"**Setting Date:** {get_val(row.get('SETTING_DATE'), True)}", unsafe_allow_html=True)

                    # FINAL QC (AK, AL, AN)
                    st.markdown("#### FINAL QC")
                    f1, f2, f3 = st.columns(3)
                    f1.write(f"**Person Name:** {get_val(row.get('FINAL_QC'))}", unsafe_allow_html=True)
                    f2.write(f"**Final Wt:** {get_val(row.get('FINAL_WT'))}", unsafe_allow_html=True)
                    f3.write(f"**Date:** {get_val(row.get('FINAL_QC_DATE'), True)}", unsafe_allow_html=True)

                    # 3. MOVEMENT LOGS
                    st.divider()
                    
                    def show_move_table(df_move, title, bag):
                        st.subheader(title)
                        if df_move is not None:
                            # Filter by Bag (Col index 3)
                            res = df_move[df_move[3].astype(str) == bag].copy()
                            if not res.empty:
                                # Swap Columns: Date(0), Time(1), Process(2)
                                in_tab = res[res[4].str.contains('IN', case=False, na=False)][[0, 1, 2]]
                                out_tab = res[res[4].str.contains('OUT', case=False, na=False)][[0, 1, 2]]
                                
                                c1, c2 = st.columns(2)
                                with c1:
                                    st.write("**INWARD**")
                                    st.table(in_tab.rename(columns={0:'DATE', 1:'TIME', 2:'PROCESS'}))
                                with c2:
                                    st.write("**OUTWARD**")
                                    st.table(out_tab.rename(columns={0:'DATE', 1:'TIME', 2:'PROCESS'}))
                            else: st.write("No record.")

                    show_move_table(df_pre, "PRE FINISH MOVEMENT", search_bag)
                    show_move_table(df_post, "POST FINISH MOVEMENT", search_bag)
                else: st.error("Bag not found.")
