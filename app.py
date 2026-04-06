import streamlit as st
import pandas as pd
import requests

st.set_page_config(page_title="MASTER DATABASE", layout="wide")
API_URL = "https://script.google.com/macros/s/AKfycbyGUZnqcp5R7k4Aa1wWvVLs-AfheyyOVeSYDkfsfVLRfe43gYg1vZ80RmHGm4Wg2ama/exec"

@st.cache_data(ttl=60) 
def fetch_data():
    try:
        response = requests.get(API_URL, timeout=15)
        return pd.DataFrame(response.json())
    except Exception as e:
        st.error(f"Waiting for Data... {e}")
        return None

df = fetch_data()

# --- SIDEBAR: MANAGEMENT SECTION ---
st.sidebar.title("📑 METAL REQUIREMENTS")
st.sidebar.info("Criteria: Metal Issue Date is Pending")

if df is not None:
    # 1. Logic: Filter for Metal Pending (Where Metal_Issue_Date is empty or null)
    # Note: We use .fillna('') to handle any missing data safely
    pending_metal_df = df[df['Metal_Issue_Date'].fillna('').str.strip() == ""]
    
    # 2. Split into Customer vs Stock
    cust_pend = pending_metal_df[pending_metal_df['Order_Type'].str.contains('CUSTOMER', case=False, na=False)]
    stock_pend = pending_metal_df[pending_metal_df['Order_Type'].str.contains('STOCK', case=False, na=False)]

    # 3. Create Sidebar Tables
    st.sidebar.subheader("📍 Customer Orders")
    if not cust_pend.empty:
        # Grouping by Customer, counting Bag_No, summing 'Metal' column
        cust_sum = cust_pend.groupby('Customer').agg({'Bag_No': 'count', 'Metal': 'sum'})
        cust_sum.columns = ['Qty (Bags)', 'Metal 18kt']
        st.sidebar.dataframe(cust_sum, use_container_width=True)
    else:
        st.sidebar.write("No Pending Customer Metal")

    st.sidebar.subheader("📍 Stock Orders")
    if not stock_pend.empty:
        stock_sum = stock_pend.groupby('Customer').agg({'Bag_No': 'count', 'Metal': 'sum'})
        stock_sum.columns = ['Qty (Bags)', 'Metal 18kt']
        st.sidebar.dataframe(stock_sum, use_container_width=True)
    else:
        st.sidebar.write("No Pending Stock Metal")

# --- MAIN PAGE ---
st.title("💎 MASTER DATABASE")

if df is not None:
    # Search and Main View
    search = st.text_input("🔍 Search by Style No or Bag No")
    
    display_df = df.copy()
    if search:
        display_df = display_df[display_df['Style_No'].str.contains(search, case=False, na=False) | 
                                display_df['Bag_No'].astype(str).str.contains(search, case=False, na=False)]

    st.dataframe(
        display_df,
        column_config={
            "Thumbnail_Link": st.column_config.ImageColumn("Preview"),
            "CAD_Link": st.column_config.LinkColumn("CAD")
        },
        use_container_width=True,
        hide_index=True
    )
