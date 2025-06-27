import pandas as pd
import numpy as np
import json

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

    def load_data(self, file, file_type):
        """Load uploaded file into DataFrame"""
        try:
            if file_type == "JSON":
                data = json.load(file)
                return pd.json_normalize(data)
            return pd.read_csv(file)
        except Exception as e:
            raise ValueError(f"Error loading file: {str(e)}")

    def clean_data(self, df):
        """Main data cleaning pipeline"""
        if df is None or df.empty:
            return df

        # Standardize column names
        df = self._standardize_columns(df)
        
        # Clean specific fields
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        if 'department' in df.columns:
            df['department'] = df['department'].fillna('Unknown').str.title()
        
        if 'ai_tool' in df.columns:
            df['ai_tool'] = self._clean_ai_tools(df['ai_tool'])
        
        if 'ease_of_use' in df.columns:
            df['ease_of_use'] = self._clean_ratings(df['ease_of_use'])
        
        return df.dropna(how='all', axis=1)

    def _standardize_columns(self, df):
        """Map columns to standardized names"""
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

    def _clean_ai_tools(self, series):
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

    def _clean_ratings(self, series, scale=5):
        """Ensure ratings are within 1-5 scale"""
        series = pd.to_numeric(series, errors='coerce')
        return series.clip(1, scale)
