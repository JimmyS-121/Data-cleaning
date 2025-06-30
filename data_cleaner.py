import pandas as pd
import numpy as np
import json
import re
from typing import Dict, List, Union

class QuestionnaireCleaner:
    def __init__(self):
        # More aggressive matching patterns that will catch your exact column names
        self.column_patterns = {
            'timestamp': r'timestamp|date|time|when',
            'department': r'department|dept|which department are you in',
            'job_role': r'job.?role|position|your role|what is your role',
            'ai_tool': r'ai.?tool|what ai tool\(s\) do you use most',
            'usage_frequency': r'usage.?frequency|how often|how frequently',
            'purpose': r'purpose|use.?case|how do you use',
            'ease_of_use': r'ease.?of.?use|how easy|usability',
            'time_saved': r'time.?saved|time.?saving|productivity',
            'suggestions': r'suggestions|feedback|improvement|comments'
        }
        
        self.suggestion_patterns = {
            'no_suggestion': r'no|none|n/a|not|nothing|nil|nan|null|undefined',
            'training': r'train|guide|tutorial|doc|manual|help|learn|educate',
            'features': r'feature|function|tool|option|capability|improve|enhance|add',
            'integration': r'integrat|connect|api|system|plugin|bridge|import|export',
            'cost': r'cost|price|cheap|afford|license|subscription|fee|pay',
            'accuracy': r'reliable|accurate|quality|precise|correct|better|trust|depend'
        }

    def load_data(self, file_path: str) -> pd.DataFrame:
        """Load data from CSV or JSON"""
        try:
            if file_path.endswith('.json'):
                with open(file_path, 'r') as f:
                    data = json.load(f)
                return pd.json_normalize(data)
            else:
                return pd.read_csv(file_path)
        except Exception as e:
            raise ValueError(f"Error loading file: {str(e)}")

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Forcefully standardize ALL columns to expected names"""
        if df.empty:
            raise ValueError("Empty dataframe provided")
            
        # 1. First normalize all column names to lowercase with underscores
        df.columns = [col.strip().lower().replace(' ', '_').replace('?', '') for col in df.columns]
        
        # 2. Aggressive pattern matching for your specific columns
        column_mapping = {}
        for col in df.columns:
            col_normalized = col.lower().replace('(', '').replace(')', '').replace('_', '')
            
            if re.search(r'whichdepartmentareyouin', col_normalized):
                column_mapping[col] = 'department'
            elif re.search(r'whataitool', col_normalized):
                column_mapping[col] = 'ai_tool'
            elif re.search(r'usagefreq|howoften', col_normalized):
                column_mapping[col] = 'usage_frequency'
            elif re.search(r'suggest|feedback|improve', col_normalized):
                column_mapping[col] = 'suggestions'
            # Add other specific mappings as needed
            else:
                # Fallback to pattern matching
                for std_col, pattern in self.column_patterns.items():
                    if re.search(pattern, col, re.IGNORECASE):
                        column_mapping[col] = std_col
                        break
                else:
                    column_mapping[col] = col  # Keep original if no match
        
        # Apply the mapping
        df = df.rename(columns=column_mapping)
        
        # 3. Data cleaning
        if 'ai_tool' in df.columns:
            df['ai_tool'] = self._clean_ai_tools(df['ai_tool'])
        
        if 'usage_frequency' in df.columns:
            df['usage_frequency'] = df['usage_frequency'].fillna('Unknown').str.strip().str.title()
        
        if 'suggestions' in df.columns:
            df['suggestions'] = self._clean_suggestions(df['suggestions'])
            df['suggestion_category'] = self._categorize_suggestions(df['suggestions'])
        
        return df

    def _clean_ai_tools(self, series: pd.Series) -> pd.Series:
        """Force clean AI tool names"""
        tool_map = {
            r'chat.?gpt': 'ChatGPT',
            r'gpt-?4': 'GPT-4',
            r'poe': 'Poe',
            r'google.?bard': 'Google Bard',
            r'gemini': 'Google Gemini',
            r'github.?copilot': 'GitHub Copilot',
            r'mid.?journey': 'Midjourney'
        }
        series = series.astype(str).str.strip().str.title()
        for pattern, replacement in tool_map.items():
            series = series.str.replace(pattern, replacement, regex=True)
        return series

    def _clean_suggestions(self, series: pd.Series) -> pd.Series:
        """Clean suggestion text"""
        series = series.astype(str).str.strip()
        series = series.replace([
            'nan', 'none', 'n/a', 'no', 'nothing', 
            'nil', 'null', 'undefined', 'not applicable'
        ], 'No suggestions', regex=True)
        return series.str.title()

    def _categorize_suggestions(self, series: pd.Series) -> pd.Series:
        """Categorize suggestions"""
        def _categorize(text):
            text = str(text).lower()
            if 'no suggestion' in text:
                return 'No suggestions'
            elif any(word in text for word in ['train', 'guide', 'learn']):
                return 'Training'
            elif any(word in text for word in ['feature', 'improve', 'add']):
                return 'Features'
            elif any(word in text for word in ['integrat', 'connect', 'api']):
                return 'Integration'
            elif any(word in text for word in ['cost', 'price', 'free']):
                return 'Cost'
            else:
                return 'Other'
        return series.apply(_categorize)

    def save_clean_data(self, df: pd.DataFrame, output_path: str):
        """Save cleaned data with guaranteed correct columns"""
        cleaned = self.clean_data(df)
        required_columns = [
            'timestamp', 'department', 'job_role', 'ai_tool',
            'usage_frequency', 'purpose', 'ease_of_use',
            'time_saved', 'suggestions'
        ]
        
        # Ensure all expected columns exist
        for col in required_columns:
            if col not in cleaned.columns:
                cleaned[col] = None  # Add missing columns as null
                
        cleaned.to_csv(output_path, index=False)

if __name__ == '__main__':
    cleaner = QuestionnaireCleaner()
    try:
        raw_data = cleaner.load_data("your_questionnaire.csv")
        cleaner.save_clean_data(raw_data, "ANALYSIS_READY.csv")
        print("Success! Cleaned data saved as ANALYSIS_READY.csv")
    except Exception as e:
        print(f"Error: {str(e)}")
