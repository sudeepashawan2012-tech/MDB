import streamlit as st
import pandas as pd
import requests

# 1. PAGE CONFIGURATION
st.set_page_config(page_title="JDS Workshop Master", layout="wide")

# This is your private link to your Google Sheet data
API_URL = "https://script.google.com/macros/s/AKfycbwalHjQW1Pzz0LaJZW18McH2huQiCSNIoiVD83TPPzacSAHnrObe9E6lL6oNTTEavMX/exec"

# 2. THE LIGHTNING FAST DATA ENGINE
@st.cache_data(ttl=300) 
def fetch_data():
    try:
        # We use 'follow_redirects=True' for Google Apps Script URLs
        response = requests.get(API_URL, timeout=15)
        response.raise_for_status() # Check if the link is working
        data = response.json()
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"Waiting for Data... (Error: {e})")
        return None

# Load the data into the app memory
df = fetch_data()

# 3. THE DASHBOARD INTERFACE
st.title("💎 MASTER DATABASE")
st.write("Live Production Data from Master Sheet")

if df is not None:
    # Sidebar Search
    search = st.sidebar.text_input("Search Style or Bag Number")
    
    # Filter Logic
    if search:
        display_df = df[df['Style_No'].str.contains(search, case=False, na=False)]
    else:
        display_df = df

    # Display the Table
    st.dataframe(
        display_df,
        column_config={
            "Thumbnail_Link": st.column_config.ImageColumn("Design Preview"),
            "CAD_Link": st.column_config.LinkColumn("CAD File")
        },
        use_container_width=True,
        hide_index=True
    )