import streamlit as st
from data_cleaner import QuestionnaireCleaner
import pandas as pd

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

def show_results(cleaner, df):
    """Display cleaning results"""
    with st.spinner("🧼 Cleaning data..."):
        cleaned_df = cleaner.clean_data(df)
    
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
    cleaner = QuestionnaireCleaner()
    
    uploaded_file = show_upload_section()
    
    if uploaded_file:
        try:
            df = cleaner.load_data(uploaded_file, "JSON" if uploaded_file.type == "application/json" else "CSV")
            
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
