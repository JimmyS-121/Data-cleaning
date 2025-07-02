import streamlit as st
from data_cleaner import QuestionnaireCleaner
import pandas as pd
import time

@st.cache_resource
def get_cleaner():
    """Cache the cleaner instance to avoid reinitialization"""
    return QuestionnaireCleaner()

def show_upload_section():
    """File upload UI component"""
    with st.expander("📤 Upload Data", expanded=True):
        col1, col2 = st.columns([1, 3])
        with col1:
            file_type = st.radio(
                "File type:",
                ["CSV", "JSON"],
                horizontal=True
            )
        with col2:
            return st.file_uploader(
                f"Choose {file_type} file",
                type=["csv", "json"],
                key="file_uploader"
            )

@st.cache_data(show_spinner="🧼 Cleaning data...")
def clean_data(_cleaner, df):
    """Cache the cleaning operation"""
    return _cleaner.clean_data(df)

def show_results(cleaner, df):
    """Display cleaning results"""
    cleaned_df = clean_data(cleaner, df)
    
    st.subheader("✨ Cleaned Data Preview")
    st.dataframe(cleaned_df.head(3))
    
    # Statistics card
    with st.expander("📊 Data Summary"):
        if 'ease_of_use' in cleaned_df.columns:
            avg_rating = cleaned_df['ease_of_use'].mean()
            st.metric("Average Ease of Use", f"{avg_rating:.1f}/5")
        
        col1, col2 = st.columns(2)
        with col1:
            st.write("**Column Types:**")
            st.json(cleaned_df.dtypes.astype(str).to_dict())
        with col2:
            st.write("**Missing Values:**")
            st.json(cleaned_df.isna().sum().to_dict())
    
    # Download
    csv = cleaned_df.to_csv(index=False)
    st.download_button(
        label="📥 Download Cleaned CSV",
        data=csv,
        file_name="cleaned_questionnaire.csv",
        mime="text/csv",
        type="primary"
    )

def main():
    st.set_page_config(
        page_title="AI Tools Data Cleaner",
        page_icon="🧹",
        layout="wide"
    )
    
    st.title("🧹 AI Tools Questionnaire Data Cleaner")
    st.caption("Upload your questionnaire data to clean and standardize it")
    
    cleaner = get_cleaner()
    uploaded_file = show_upload_section()
    
    if uploaded_file:
        try:
            file_type = "JSON" if uploaded_file.type == "application/json" else "CSV"
            df = cleaner.load_data(uploaded_file, file_type)
            
            with st.expander("🔍 Raw Data Preview"):
                st.dataframe(df.head(3))
            
            show_results(cleaner, df)
            
        except ValueError as e:
            st.error(f"❌ Error: {str(e)}")
            st.info("Please check your file and try again.")
        except Exception as e:
            st.error(f"⚠️ Unexpected error: {str(e)}")
            st.stop()

if __name__ == "__main__":
    main()
