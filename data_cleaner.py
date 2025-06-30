import pandas as pd
import numpy as np
import re

class QuestionnaireCleaner:
    def __init__(self):
        # Mapping between original columns and analysis-friendly names
        self.column_mapping = {
            'Timestamp': 'timestamp',
            'Which Department are you in?': 'department',
            'Role:': 'job_role',
            'What AI tool(s) do you use most? (e.g. Poe/ChatGPT)': 'ai_tool',
            'Usage of AI Tools': 'usage_frequency',
            'Purpose of using AI tools': 'purpose',
            'Ease of use (Rating 1-5, 5 = easiest)': 'ease_of_use',
            'How efficiency do the AI tools works for your tasks? (5 = most efficient)': 'time_saved',
            'Any suggestions or improvement for us about the AI tools or our website (ProjectFly)?': 'suggestions'
        }

        # AI tool standardization
        self.tool_standardization = {
            r'chat.?gpt': 'ChatGPT',
            r'poe\b': 'Poe',
            r'canav?a': 'Canva',
            r'mid.?journey': 'Midjourney',
            r'deep.?seek': 'Deepseek',
            r'kling.?ai': 'Kling AI',
            r'copilot': 'Copilot',
            r'gamma': 'Gamma'
        }

    def load_data(self, file_path: str) -> pd.DataFrame:
        """Load data from CSV file"""
        try:
            df = pd.read_csv(file_path)
            if df.empty:
                raise ValueError("Empty file uploaded")
            return df
        except Exception as e:
            raise ValueError(f"Error loading file: {str(e)}")

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add analysis-friendly columns while preserving original data"""
        df_clean = df.copy()
        
        # Add new standardized columns for analysis
        for original_col, new_col in self.column_mapping.items():
            if original_col in df_clean.columns:
                df_clean[new_col] = df_clean[original_col]
        
        # Standardize AI tools in the new column
        if 'ai_tool' in df_clean.columns:
            df_clean['ai_tool'] = self._clean_ai_tools(df_clean['ai_tool'])
        
        # Standardize usage frequency
        if 'usage_frequency' in df_clean.columns:
            df_clean['usage_frequency'] = self._clean_usage_frequency(df_clean['usage_frequency'])
        
        # Clean ratings (1-5 scale)
        for rating_col in ['ease_of_use', 'time_saved']:
            if rating_col in df_clean.columns:
                df_clean[rating_col] = self._clean_ratings(df_clean[rating_col])
        
        # Clean suggestions
        if 'suggestions' in df_clean.columns:
            df_clean['suggestions'] = self._clean_suggestions(df_clean['suggestions'])
        
        return df_clean

    def _clean_ai_tools(self, series: pd.Series) -> pd.Series:
        """Standardize AI tool names"""
        def standardize_tool(tool_name: str) -> str:
            if pd.isna(tool_name):
                return 'Unknown'
            tool_name = str(tool_name).lower().strip()
            for pattern, standardized_name in self.tool_standardization.items():
                if re.search(pattern, tool_name, re.IGNORECASE):
                    return standardized_name
            return tool_name.title()
        return series.apply(standardize_tool)

    def _clean_usage_frequency(self, series: pd.Series) -> pd.Series:
        """Clean usage frequency values"""
        freq_map = {'daily': 'Daily', 'weekly': 'Weekly', 'monthly': 'Monthly', 'rarely': 'Rarely'}
        return series.str.lower().map(freq_map).fillna(series.str.title())

    def _clean_suggestions(self, series: pd.Series) -> pd.Series:
        """Clean suggestions column"""
        return series.str.strip().replace(
            r'^(no|none|n/a|not|nothing|nil|nan|null|undefined)$', 
            'No suggestions', 
            regex=True
        )

    def _clean_ratings(self, series: pd.Series) -> pd.Series:
        """Ensure ratings are numeric between 1-5"""
        return pd.to_numeric(series, errors='coerce').clip(1, 5)

    def save_cleaned_data(self, df: pd.DataFrame, output_path: str):
        """Save data with both original and analysis columns"""
        df.to_csv(output_path, index=False, encoding='utf-8-sig')

if __name__ == '__main__':
    cleaner = QuestionnaireCleaner()
    
    try:
        # Load original data
        raw_data = cleaner.load_data("cleaned_questionnaire (1).csv")
        
        # Add analysis columns while preserving originals
        cleaned_data = cleaner.clean_data(raw_data)
        
        # Save the enhanced data
        cleaner.save_cleaned_data(cleaned_data, "cleaned_questionnaire_ANALYSIS_READY.csv")
        
        print("="*50)
        print("SUCCESS: Output saved with both original and analysis columns")
        print("Original columns preserved, added analysis-friendly columns:")
        print([col for col in cleaned_data.columns if col in cleaner.column_mapping.values()])
        print("="*50)
        
    except Exception as e:
        print(f"ERROR: {str(e)}")
