import pandas as pd
import re
from typing import Dict, Union, Optional
import json
from datetime import datetime

class QuestionnaireCleaner:
    def __init__(self):
        # Pre-compile regex patterns for better performance
        self._compiled_patterns = {}
        
        # Target column mapping
        self.target_columns = {
            'timestamp': ['timestamp', 'date', 'time', 'datetime', 'when'],
            'department': ['department', 'dept', 'which department', 'your department'],
            'job_role': ['job_role', 'role', 'position', 'your role', 'job title'],
            'ai_tool': ['ai_tool', 'what ai tool', 'ai tool used', 'tools you use', 'what tools do you use'],
            'usage_frequency': ['usage_frequency', 'usage of ai tools', 'how often', 'frequency', 'usage frequency'],
            'purpose': ['purpose', 'purpose of using ai tools', 'why do you use', 'main purpose'],
            'ease_of_use': ['ease_of_use', 'ease of use', 'how easy', 'user friendly'],
            'efficiency': ['efficiency', 'how efficiency', 'productivity impact', 'time saved'],
            'suggestions': ['suggestions', 'any suggestions', 'feedback', 'comments']
        }
        
        # Standardization rules with pre-compiled patterns
        self.standardization_rules = {
            'ai_tool': {
                re.compile(r'\bchatgpt\b', re.IGNORECASE): 'ChatGPT',
                re.compile(r'\bpoe\b', re.IGNORECASE): 'Poe',
                re.compile(r'\bcanava\b', re.IGNORECASE): 'Canva',
                re.compile(r'\bgamma\b', re.IGNORECASE): 'Gamma',
                re.compile(r'\bmidjourney\b', re.IGNORECASE): 'Midjourney',
                re.compile(r'\bcopilot\b', re.IGNORECASE): 'Copilot',
                re.compile(r'\bkling ai\b', re.IGNORECASE): 'Kling AI',
                re.compile(r'\bdeepseek\b', re.IGNORECASE): 'Deepseek'
            },
            'usage_frequency': {
                re.compile(r'\bweekly\b', re.IGNORECASE): 'Weekly',
                re.compile(r'\bmonthly\b', re.IGNORECASE): 'Monthly',
                re.compile(r'\bdaily\b', re.IGNORECASE): 'Daily',
                re.compile(r'\brarely\b', re.IGNORECASE): 'Rarely',
                re.compile(r'\bnever\b', re.IGNORECASE): 'Never'
            }
        }

    def load_data(self, file_path: Union[str, object], file_type: str = "CSV") -> pd.DataFrame:
        """Load data from file (CSV or JSON) with optimized parameters"""
        try:
            if file_type.upper() == "CSV":
                df = pd.read_csv(file_path, engine='pyarrow', dtype_backend='pyarrow')
            elif file_type.upper() == "JSON":
                df = pd.read_json(file_path)
            else:
                raise ValueError("Unsupported file type. Use CSV or JSON.")
            
            if df.empty:
                raise ValueError("Empty file uploaded")
            return df
        except Exception as e:
            raise ValueError(f"Error loading file: {str(e)}")

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize the dataframe"""
        if df.empty:
            raise ValueError("Empty dataframe provided")
        
        # Step 1: Standardize column names
        df = self._standardize_column_names(df)
        
        # Step 2: Clean each column
        for col in df.columns:
            if col in self.standardization_rules:
                df[col] = self._standardize_values(df[col], col)
        
        # Step 3: Ensure proper data types
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        
        numeric_cols = ['ease_of_use', 'efficiency']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df

    def _standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert input columns to target column names with optimized matching"""
        column_mapping = {}
        col_lower_cache = {}
        
        # Pre-cache lowercase column names
        for col in df.columns:
            col_lower_cache[col] = str(col).lower().strip()
        
        for col, col_lower in col_lower_cache.items():
            matched = False
            
            for target_col, variations in self.target_columns.items():
                if any(variation.lower() in col_lower for variation in variations):
                    column_mapping[col] = target_col
                    matched = True
                    break
            
            if not matched:
                # Fallback matching with regex for better performance
                if re.search(r'tool|use|ai', col_lower):
                    column_mapping[col] = 'ai_tool'
                elif re.search(r'freq|often', col_lower):
                    column_mapping[col] = 'usage_frequency'
                elif re.search(r'ease|easy', col_lower):
                    column_mapping[col] = 'ease_of_use'
                elif re.search(r'efficien|productiv', col_lower):
                    column_mapping[col] = 'efficiency'
                else:
                    column_mapping[col] = col
        
        return df.rename(columns=column_mapping)

    def _standardize_values(self, series: pd.Series, column: str) -> pd.Series:
        """Standardize values in a column using pre-compiled patterns"""
        if column not in self.standardization_rules:
            return series
        
        cleaned = series.astype(str).str.strip()
        
        for pattern, replacement in self.standardization_rules[column].items():
            cleaned = cleaned.str.replace(
                pattern,
                replacement,
                regex=True
            )
        
        return cleaned

    def save_cleaned_data(self, df: pd.DataFrame, output_path: str):
        """Save cleaned data to CSV with optimized parameters"""
        cleaned_df = self.clean_data(df)
        cleaned_df.to_csv(output_path, index=False, encoding='utf-8-sig')

# For backward compatibility
DataCleaner = QuestionnaireCleaner
