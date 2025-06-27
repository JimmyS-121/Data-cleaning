import streamlit as st
from data_cleaner import QuestionnaireCleaner
import pandas as pd
import json
from io import StringIO

def main():
    st.set_page_config(
        page_title="AI Tools Data Cleaner",
        page_icon="🧹",
        layout="wide"
    )
    
    st.title("🧹 AI Tools Questionnaire Data Cleaner")
    st.markdown("""
    Upload your raw questionnaire data (CSV or JSON) to get analysis-ready data.
    """)
    
    cleaner = QuestionnaireCleaner()
    
    # File upload section
    with st.expander("📤 Upload Data", expanded=True):
        file_type = st.radio(
            "File type:",
            ["CSV", "JSON"],
            horizontal=True
        )
        
        uploaded_file = st.file_uploader(
            f"Choose {file_type} file",
            type=["csv", "json"],
            key="file_uploader"
        )
    
    if uploaded_file:
        try:
            # Load data based on file type
            if file_type == "CSV":
                df = pd.read_csv(uploaded_file)
            else:
                df = pd.read_json(uploaded_file)
            
            # Show raw data preview
            with st.expander("🔍 Raw Data Preview"):
                st.dataframe(df.head())
            
            # Clean data
            with st.spinner("🧼 Cleaning data..."):
                cleaned_df = cleaner.clean_data(df)
            
            # Show cleaned data
            st.subheader("✨ Cleaned Data")
            st.dataframe(cleaned_df.head())
            
            # Download button
            csv = cleaned_df.to_csv(index=False)
            st.download_button(
                label="📥 Download Cleaned CSV",
                data=csv,
                file_name="cleaned_ai_questionnaire.csv",
                mime="text/csv"
            )
            
        except Exception as e:
            st.error(f"❌ Error processing file: {str(e)}")
            st.info("""
            **Common issues:**
            - JSON file not in valid format
            - CSV missing required columns
            - Date formats not recognized
            """)

if __name__ == "__main__":
    main()
