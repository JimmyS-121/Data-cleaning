import streamlit as st
from data_cleaner import QuestionnaireCleaner
import pandas as pd

def main():
    st.set_page_config(
        page_title="AI Tools Data Cleaner",
        page_icon="🧹",
        layout="wide"
    )
    
    st.title("🧹 AI Tools Questionnaire Data Cleaner")
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
            # Load data
            df = cleaner.load_data(uploaded_file, file_type)
            
            # Show raw data preview
            with st.expander("🔍 Raw Data Preview"):
                st.dataframe(df.head(3))
            
            # Clean data
            with st.spinner("🧼 Cleaning data..."):
                cleaned_df = cleaner.clean_data(df)
            
            # Show cleaned data
            st.subheader("✨ Cleaned Data")
            st.dataframe(cleaned_df.head(3))
            
            # Download button
            csv = cleaned_df.to_csv(index=False)
            st.download_button(
                label="📥 Download Cleaned CSV",
                data=csv,
                file_name="cleaned_questionnaire.csv",
                mime="text/csv"
            )
            
        except Exception as e:
            st.error(f"Error: {str(e)}")
            st.info("Please check your file format and try again.")

if __name__ == "__main__":
    main()
