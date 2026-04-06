import streamlit as st
import pandas as pd
import requests

st.set_page_config(page_title="MASTER DATABASE", layout="wide")
API_URL = "https://script.google.com/macros/s/AKfycbzJeiT_mTmPFVEFDqDZvnZeakdFVxUrGiOjtl-NBgGFHyi3HYLCO1648JSm7s2bW0A/exec"

@st.cache_data(ttl=60) 
def fetch_data():
    try:
        response = requests.get(API_URL, timeout=15)
        return pd.DataFrame(response.json())
    except Exception as e:
        return None

df = fetch_data()

# --- SIDEBAR: MANAGEMENT REPORTS ---
st.sidebar.title("📊 METAL REQUIREMENTS")
st.sidebar.markdown("*(Bags with Metal Pending)*")

if df is not None:
    # 1. CRITERIA: Find rows where 'Metal_Issue_Date' is empty
    pending_mask = (df['Metal_Issue_Date'] == "") | (df['Metal_Issue_Date'].isna())
    pending_df = df[pending_mask].copy()
    
    # Ensure Metal is treated as a number for summing
    pending_df['Metal'] = pd.to_numeric(pending_df['Metal'], errors='coerce').fillna(0)

    # 2. CUSTOMER ORDER SECTION
    st.sidebar.subheader("💎 Customer Order Metal")
    cust_df = pending_df[pending_df['Order_Type'].str.contains('CUSTOMER', case=False, na=False)]
    if not cust_df.empty:
        cust_summary = cust_df.groupby('Customer').agg({
            'Bag_No': 'count',
            'Metal': 'sum'
        }).rename(columns={'Bag_No': 'Qty (Bags)', 'Metal': 'Metal 18kt'})
        st.sidebar.table(cust_summary)
    else:
        st.sidebar.write("No pending customer orders.")

    # 3. STOCK ORDER SECTION
    st.sidebar.subheader("📦 Stock Order Metal")
    stock_df = pending_df[pending_df['Order_Type'].str.contains('STOCK', case=False, na=False)]
    if not stock_df.empty:
        stock_summary = stock_df.groupby('Customer').agg({
            'Bag_No': 'count',
            'Metal': 'sum'
        }).rename(columns={'Bag_No': 'Qty (Bags)', 'Metal': 'Metal 18kt'})
        st.sidebar.table(stock_summary)
    else:
        st.sidebar.write("No pending stock orders.")

# --- MAIN PAGE ---
st.title("💎 MASTER DATABASE")

if df is not None:
    search = st.text_input("🔍 Quick Search (Style or Bag No)")
    
    display_df = df.copy()
    if search:
        display_df = display_df[
            display_df['Style_No'].str.contains(search, case=False, na=False) | 
            display_df['Bag_No'].astype(str).str.contains(search, case=False, na=False)
        ]

    st.dataframe(
        display_df,
        column_config={
            "Thumbnail_Link": st.column_config.ImageColumn("Preview"),
            "CAD_Link": st.column_config.LinkColumn("CAD")
        },
        use_container_width=True,
        hide_index=True
    )
