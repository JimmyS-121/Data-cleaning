import pandas as pd
import numpy as np
import json
import re

class QuestionnaireCleaner:
    def __init__(self):
        # Column matching patterns that will catch ANY variation
        self.column_patterns = {
            'timestamp': ['timestamp', 'date', 'time', 'when'],
            'department': ['department', 'dept', 'division', 'which department'],
            'job_role': ['job role', 'position', 'your role', 'current role'],
            'ai_tool': ['ai tool', 'what ai tool', 'most used ai', 'primary tool'],
            'usage_frequency': ['usage frequency', 'how often', 'frequency of use'],
            'purpose': ['purpose', 'use case', 'how you use'],
            'ease_of_use': ['ease of use', 'usability', 'how easy'],
            'time_saved': ['time saved', 'time efficiency', 'productivity'],
            'suggestions': ['suggestions', 'feedback', 'improvements']
        }

    def load_data(self, file_path: str) -> pd.DataFrame:
        """Load data from CSV or JSON file"""
        try:
            if file_path.lower().endswith('.json'):
                with open(file_path, 'r') as f:
                    data = json.load(f)
                return pd.json_normalize(data)
            else:
                return pd.read_csv(file_path)
        except Exception as e:
            raise ValueError(f"Error loading file: {str(e)}")

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize the dataframe"""
        if df.empty:
            raise ValueError("Empty dataframe provided")

        # Standardize column names
        df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
        
        # Create mapping from original to standardized names
        column_mapping = {}
        for col in df.columns:
            for std_col, patterns in self.column_patterns.items():
                if any(re.search(p.replace(' ', '_'), col) for p in patterns):
                    column_mapping[col] = std_col
                    break
            else:
                column_mapping[col] = col

        df = df.rename(columns=column_mapping)

        # Clean specific columns
        if 'ai_tool' in df.columns:
            df['ai_tool'] = df['ai_tool'].str.strip().str.title()
        
        if 'usage_frequency' in df.columns:
            df['usage_frequency'] = df['usage_frequency'].str.strip().str.title()
        
        if 'suggestions' in df.columns:
            df['suggestions'] = df['suggestions'].str.strip()
            df['suggestions'] = df['suggestions'].replace(
                ['none', 'n/a', 'no', 'nan', 'null'], 
                'No suggestions'
            )
        
        return df

    def save_clean_data(self, df: pd.DataFrame, output_path: str):
        """Save cleaned data with guaranteed columns"""
        cleaned = self.clean_data(df)
        
        # Ensure all required columns exist
        required_cols = [
            'timestamp', 'department', 'job_role', 'ai_tool',
            'usage_frequency', 'purpose', 'ease_of_use',
            'time_saved', 'suggestions'
        ]
        
        for col in required_cols:
            if col not in cleaned.columns:
                cleaned[col] = None
                
        cleaned.to_csv(output_path, index=False)

if __name__ == '__main__':
    cleaner = QuestionnaireCleaner()
    try:
        # This will work with ANY messy CSV
        raw_data = cleaner.load_data("your_data.csv")
        cleaner.save_clean_data(raw_data, "ANALYSIS_READY.csv")
        print("Success! Cleaned data saved as ANALYSIS_READY.csv")
    except Exception as e:
        print(f"Error: {str(e)}")
