import pandas as pd
import numpy as np
import json
import re
from typing import Dict, List, Union

class QuestionnaireCleaner:
    def __init__(self):
        # Enhanced standardization mappings
        self.standard_columns = {
            'timestamp': ['timestamp', 'date', 'time', 'datetime'],
            'department': ['department', 'dept', 'division', 'team', 'which department'],
            'job_role': ['job_role', 'role', 'position', 'job', 'job role', 'role:'],
            'ai_tool': ['ai_tool', 'what ai tool', 'ai tool used', 'tool', 'ai'],
            'usage_frequency': ['usage_frequency', 'usage of ai tools', 'frequency', 'usage', 'how often'],
            'purpose': ['purpose', 'purpose of using ai tools', 'use case', 'application', 'used for'],
            'ease_of_use': ['ease_of_use', 'ease', 'usability', 'ease of use'],
            'efficiency': ['efficiency', 'how efficiency', 'time_saved', 'time save', 'time saving'],
            'suggestions': ['suggestions', 'improvement', 'feedback', 'comments', 'any suggestions']
        }
        
        # Tool name standardization
        self.tool_map = {
            r'chat.?gpt': 'ChatGPT',
            r'gpt-?4': 'ChatGPT-4',
            r'poe': 'Poe',
            r'canva': 'Canva',
            r'canava': 'Canva',
            r'gamma': 'Gamma',
            r'mid.?journey': 'Midjourney',
            r'copilot': 'Copilot',
            r'kling.?ai': 'Kling AI',
            r'deep.?seek': 'Deepseek'
        }

        # Usage frequency standardization
        self.usage_map = {
            r'daily': 'Daily',
            r'weekly': 'Weekly',
            r'monthly': 'Monthly',
            r'rarely': 'Rarely',
            r'^no': 'Never',
            r'^yes': 'Regularly'
        }

    def load_data(self, file_path: str, file_type: str = "csv") -> pd.DataFrame:
        """Load data from file with more flexible parameters"""
        try:
            file_type = file_type.lower()
            if file_type == "json":
                with open(file_path, 'r') as f:
                    data = json.load(f)
                df = pd.json_normalize(data)
            else:  # default to CSV
                df = pd.read_csv(file_path)
            
            if df.empty:
                raise ValueError("Empty file uploaded")
            return df
            
        except Exception as e:
            raise ValueError(f"Error loading file: {str(e)}")

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enhanced cleaning method with better column handling"""
        if df is None or df.empty:
            raise ValueError("No data provided or empty dataframe")
            
        df_clean = df.copy()
        df_clean = self._standardize_columns(df_clean)
        
        # Clean each column systematically
        if 'timestamp' in df_clean.columns:
            df_clean['timestamp'] = pd.to_datetime(df_clean['timestamp'], errors='coerce')
        
        if 'department' in df_clean.columns:
            df_clean['department'] = df_clean['department'].str.strip().str.title()
        
        if 'ai_tool' in df_clean.columns:
            df_clean['ai_tool'] = self._clean_ai_tools(df_clean['ai_tool'])
        
        if 'usage_frequency' in df_clean.columns:
            df_clean['usage_frequency'] = self._clean_usage_frequency(df_clean['usage_frequency'])
        
        if 'ease_of_use' in df_clean.columns:
            df_clean['ease_of_use'] = self._clean_ratings(df_clean['ease_of_use'])
        
        if 'efficiency' in df_clean.columns:
            df_clean['efficiency'] = self._clean_ratings(df_clean['efficiency'])
        
        if 'suggestions' in df_clean.columns:
            df_clean['suggestions'] = self._clean_suggestions(df_clean['suggestions'])
        
        return df_clean

    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """More robust column standardization"""
        column_mapping = {}
        
        for original_col in df.columns:
            original_lower = str(original_col).lower().strip()
            matched = False
            
            for std_col, variations in self.standard_columns.items():
                for variation in variations:
                    if variation.lower() in original_lower:
                        column_mapping[original_col] = std_col
                        matched = True
                        break
                if matched:
                    break
            
            if not matched:
                column_mapping[original_col] = original_col
        
        # Apply mapping and drop duplicates
        df = df.rename(columns=column_mapping)
        return df.loc[:, ~df.columns.duplicated()]

    def _clean_ai_tools(self, series: pd.Series) -> pd.Series:
        """Standardize AI tool names"""
        series = series.astype(str).str.strip().str.lower()
        for pattern, replacement in self.tool_map.items():
            series = series.str.replace(pattern, replacement, case=False, regex=True)
        return series.str.title()

    def _clean_usage_frequency(self, series: pd.Series) -> pd.Series:
        """Standardize usage frequency values"""
        series = series.astype(str).str.strip().str.lower()
        for pattern, replacement in self.usage_map.items():
            series = series.str.replace(pattern, replacement, case=False, regex=True)
        return series

    def _clean_ratings(self, series: pd.Series, scale: int = 5) -> pd.Series:
        """Clean and standardize rating values"""
        series = pd.to_numeric(series, errors='coerce')
        return series.clip(1, scale)

    def _clean_suggestions(self, series: pd.Series) -> pd.Series:
        """Clean suggestion text"""
        cleaned = series.astype(str).str.strip()
        cleaned = cleaned.replace(r'^no$|^none$|^n/a$|^nothing$', 'No suggestions', regex=True)
        return cleaned

    def save_cleaned_data(self, df: pd.DataFrame, output_path: str):
        """Save cleaned data to CSV"""
        cleaned_df = self.clean_data(df)
        cleaned_df.to_csv(output_path, index=False, encoding='utf-8-sig')


if __name__ == '__main__':
    cleaner = QuestionnaireCleaner()
    try:
        raw_data = cleaner.load_data("Testing use (Responses) - Form responses 1.csv")
        cleaner.save_cleaned_data(raw_data, "cleaned_questionnaire.csv")
        print("Successfully created cleaned file!")
    except Exception as e:
        print(f"Error: {str(e)}")
