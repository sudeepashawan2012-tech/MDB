import streamlit as st
import pandas as pd
import requests

st.set_page_config(page_title="MASTER DATABASE", layout="wide")
API_URL = "https://script.google.com/macros/s/AKfycbyI75L-6S2-fN6Y-N5-996_W7_h5M7pX-1yXp_C7-WpX_qXp_Q/exec"

@st.cache_data(ttl=60) 
def fetch_data():
    try:
        response = requests.get(API_URL, timeout=15)
        return pd.DataFrame(response.json())
    except Exception as e:
        return None

df = fetch_data()

# --- SIDEBAR SECTION ---
st.sidebar.title("🛠️ MANAGEMENT TOOLS")

# Create a toggle button in the sidebar
show_metal_report = st.sidebar.button("📊 METAL REQUIREMENT")

# --- MAIN PAGE ---
st.title("💎 MASTER DATABASE")

if df is not None:
    # --- LOGIC FOR THE METAL REQUIREMENT TABLE (HIDDEN BY DEFAULT) ---
    if show_metal_report:
        st.markdown("### 📋 PENDING METAL REQUIREMENTS")
        
        # Criteria: Metal_Issue_Date is empty
        pending_mask = (df['Metal_Issue_Date'] == "") | (df['Metal_Issue_Date'].isna())
        pending_df = df[pending_mask].copy()
        pending_df['Metal'] = pd.to_numeric(pending_df['Metal'], errors='coerce').fillna(0)

        # Create two columns on the main page for the tables
        m_col1, m_col2 = st.columns(2)

        with m_col1:
            st.info("**Customer Order Metal Requirement**")
            cust_df = pending_df[pending_df['Order_Type'].str.contains('CUSTOMER', case=False, na=False)]
            if not cust_df.empty:
                cust_sum = cust_df.groupby('Customer').agg({'Bag_No': 'count', 'Metal': 'sum'})
                cust_sum.columns = ['Qty (Bags)', 'Metal 18kt']
                st.table(cust_sum)
            else:
                st.write("No pending customer metal.")

        with m_col2:
            st.warning("**Stock Order Metal Requirement**")
            stock_df = pending_df[pending_df['Order_Type'].str.contains('STOCK', case=False, na=False)]
            if not stock_df.empty:
                stock_sum = stock_df.groupby('Customer').agg({'Bag_No': 'count', 'Metal': 'sum'})
                stock_sum.columns = ['Qty (Bags)', 'Metal 18kt']
                st.table(stock_sum)
            else:
                st.write("No pending stock metal.")
        
        st.markdown("---") # Visual separator

    # --- MAIN DATABASE SEARCH & VIEW ---
    search = st.text_input("🔍 Search Style or Bag Number")
    
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
