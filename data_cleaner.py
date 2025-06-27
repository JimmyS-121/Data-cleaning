import pandas as pd
import numpy as np
import json
from pathlib import Path
from datetime import datetime

class QuestionnaireCleaner:
    def __init__(self):
        self.standard_columns = {
            'timestamp': ['timestamp', 'date', 'time', 'datetime'],
            'department': ['department', 'dept', 'division', 'team'],
            'job_role': ['job_role', 'role', 'position', 'job', 'job role'],
            'ai_tool': ['ai_tool_used', 'ai_tool', 'tool', 'ai', 'ai_tool used'],
            'usage_frequency': ['usage_frequency', 'frequency', 'usage', 'how often'],
            'purpose': ['purpose', 'use case', 'application', 'used for'],
            'ease_of_use': ['ease_of_use', 'ease', 'usability', 'ease of use'],
            'time_saved': ['time_saved', 'time', 'efficiency', 'time save', 'time saving'],
            'suggestions': ['improvement_suggestion', 'suggestions', 'feedback', 'comments']
        }

    def load_data(self, file_path, file_type='auto'):
        """Load data from CSV or JSON file with automatic detection"""
        try:
            if file_type == 'json' or (file_type == 'auto' and str(file_path).lower().endswith('.json')):
                if isinstance(file_path, str):
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                else:  # For file upload objects
                    data = json.load(file_path)
                return pd.json_normalize(data)
            else:  # Default to CSV
                return pd.read_csv(file_path, parse_dates=['timestamp'], dayfirst=True)
        except Exception as e:
            print(f"Error loading file: {e}")
            return None

    def clean_data(self, df):
        """Main cleaning pipeline"""
        if df is None or df.empty:
            return df

        # 1. Standardize column names
        df = self.standardize_columns(df)
        
        # 2. Clean specific fields
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        if 'department' in df.columns:
            df['department'] = df['department'].fillna('Student').str.title()
        
        if 'ai_tool' in df.columns:
            df['ai_tool'] = self.clean_ai_tools(df['ai_tool'])
        
        if 'ease_of_use' in df.columns:
            df['ease_of_use'] = self.clean_ratings(df['ease_of_use'])
        
        return df

    def standardize_columns(self, df):
        """Auto-map columns to standard names"""
        column_mapping = {}
        for col in df.columns:
            col_lower = str(col).lower()
            for std_col, keywords in self.standard_columns.items():
                if any(kw in col_lower for kw in keywords):
                    column_mapping[col] = std_col
                    break
            else:
                column_mapping[col] = col
        return df.rename(columns=column_mapping)

    def clean_ai_tools(self, series):
        """Standardize AI tool names"""
        tool_map = {
            'chatgpt': 'ChatGPT',
            'gpt': 'ChatGPT',
            'bard': 'Google Bard',
            'gemini': 'Google Gemini',
            'copilot': 'GitHub Copilot'
        }
        return (series.astype(str).str.strip().str.lower()
                .replace(tool_map).str.title())

    def clean_ratings(self, series, scale=5):
        """Ensure ratings are 1-5 scale"""
        series = pd.to_numeric(series, errors='coerce')
        return series.clip(1, scale)

    def save_clean_data(self, df, output_path):
        """Save cleaned data to CSV"""
        df.to_csv(output_path, index=False)
        return output_path

# Self-test when run directly
if __name__ == "__main__":
    cleaner = QuestionnaireCleaner()
    
    # Test CSV
    test_csv = pd.DataFrame({
        'Timestamp': ['2023-01-01', '2023-01-02'],
        'Dept': ['IT', 'HR'],
        'AI Tool Used': ['chatgpt', 'copilot']
    })
    test_csv.to_csv('test_csv.csv', index=False)
    
    # Test JSON
    test_json = [
        {"timestamp": "2023-01-01", "department": "IT", "ai_tool": "chatgpt"},
        {"timestamp": "2023-01-02", "department": "HR", "ai_tool": "copilot"}
    ]
    with open('test_json.json', 'w') as f:
        json.dump(test_json, f)
    
    # Run tests
    print("=== CSV Test ===")
    df_csv = cleaner.load_data('test_csv.csv')
    print(cleaner.clean_data(df_csv).head())
    
    print("\n=== JSON Test ===")
    df_json = cleaner.load_data('test_json.json')
    print(cleaner.clean_data(df_json).head())
