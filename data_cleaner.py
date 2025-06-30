import pandas as pd
import numpy as np
import json
import re
from typing import Dict, List, Union

class QuestionnaireCleaner:
    def __init__(self):
        # Column name patterns that will match ANY variation of your questionnaire
        self.column_patterns = {
            'timestamp': r'timestamp|date|time|when',
            'department': r'department|dept|which\s*department',
            'job_role': r'job[\s_-]*role|position|your\s*role|what\s*is\s*your\s*role',
            'ai_tool': r'ai[\s_-]*tool|what\s*ai\s*tool|most\s*used\s*ai',
            'usage_frequency': r'usage[\s_-]*frequency|how\s*often|how\s*frequently',
            'purpose': r'purpose|use[\s_-]*case|how\s*do\s*you\s*use',
            'ease_of_use': r'ease[\s_-]*of[\s_-]*use|how\s*easy|usability',
            'time_saved': r'time[\s_-]*saved|time[\s_-]*saving|productivity',
            'suggestions': r'suggestions|feedback|improvement|comments|what\s*improvements'
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
        """SMASHES through any CSV/JSON file and loads it"""
        try:
            if file_path.lower().endswith('.json'):
                with open(file_path, 'r') as f:
                    data = json.load(f)
                df = pd.json_normalize(data)
            else:
                # EATS CSV files for breakfast
                df = pd.read_csv(file_path)
            
            if df.empty:
                raise ValueError("Empty file! Feed me data!")
            return df
            
        except Exception as e:
            raise ValueError(f"CRASHED while loading file: {str(e)}")

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """DESTROYS messy data and BUILDS clean dataframe"""
        if df.empty:
            raise ValueError("I can't clean what doesn't exist!")
            
        # 1. SMASH column names into consistent format
        df.columns = [col.strip().lower().replace(' ', '_').replace('?', '') 
                     for col in df.columns]
        
        # 2. OBLITERATE original column names and create perfect standardized ones
        column_mapping = {}
        for col in df.columns:
            # NORMALIZE the hell out of column names
            normalized_col = re.sub(r'[^a-z0-9]', '', col.lower())
            
            # BRUTAL pattern matching that WILL work
            if re.search(r'whichdepartment', normalized_col):
                column_mapping[col] = 'department'
            elif re.search(r'whataitool|mostusedai', normalized_col):
                column_mapping[col] = 'ai_tool'
            elif re.search(r'usagefreq|howoften', normalized_col):
                column_mapping[col] = 'usage_frequency'
            elif re.search(r'suggest|feedback|improve', normalized_col):
                column_mapping[col] = 'suggestions'
            else:
                # FALLBACK to nuclear pattern matching
                for std_col, pattern in self.column_patterns.items():
                    if re.search(pattern, col, re.IGNORECASE):
                        column_mapping[col] = std_col
                        break
                else:
                    column_mapping[col] = col  # Keep original if no match
        
        # 3. ANNIHILATE the old column names
        df = df.rename(columns=column_mapping)
        
        # 4. CLEANSE the data with FIRE
        if 'ai_tool' in df.columns:
            df['ai_tool'] = self._clean_ai_tools(df['ai_tool'])
        
        if 'usage_frequency' in df.columns:
            df['usage_frequency'] = (df['usage_frequency']
                                    .fillna('Unknown')
                                    .str.strip()
                                    .str.title())
        
        if 'suggestions' in df.columns:
            df['suggestions'] = self._clean_suggestions(df['suggestions'])
            df['suggestion_category'] = self._categorize_suggestions(df['suggestions'])
        
        return df

    def _clean_ai_tools(self, series: pd.Series) -> pd.Series:
        """MURDERS inconsistent AI tool names"""
        tool_map = {
            r'chat.?gpt': 'ChatGPT',
            r'gpt-?4': 'GPT-4',
            r'poe': 'Poe',
            r'google.?bard': 'Google Bard',
            r'gemini': 'Google Gemini',
            r'github.?copilot': 'GitHub Copilot',
            r'mid.?journey': 'Midjourney',
            r'claude': 'Claude'
        }
        series = series.astype(str).str.strip().str.title()
        for pattern, replacement in tool_map.items():
            series = series.str.replace(pattern, replacement, regex=True)
        return series

    def _clean_suggestions(self, series: pd.Series) -> pd.Series:
        """PURIFIES suggestion text"""
        series = series.astype(str).str.strip()
        series = series.replace([
            'nan', 'none', 'n/a', 'no', 'nothing', 
            'nil', 'null', 'undefined', 'not applicable'
        ], 'No suggestions', regex=True)
        return series.str.title()

    def _categorize_suggestions(self, series: pd.Series) -> pd.Series:
        """ORGANIZES suggestions with MILITARY precision"""
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
        """EXPORTS pristine data ready for ANALYSIS DOMINATION"""
        cleaned = self.clean_data(df)
        
        # GUARANTEED columns your analysis tool needs
        required_columns = [
            'timestamp', 'department', 'job_role', 'ai_tool',
            'usage_frequency', 'purpose', 'ease_of_use',
            'time_saved', 'suggestions'
        ]
        
        # FORCE missing columns to exist
        for col in required_columns:
            if col not in cleaned.columns:
                cleaned[col] = None
                
        cleaned.to_csv(output_path, index=False)

if __name__ == '__main__':
    cleaner = QuestionnaireCleaner()
    try:
        # CRUSH your data file and BUILD analysis-ready version
        raw_data = cleaner.load_data("your_questionnaire.csv")
        cleaner.save_clean_data(raw_data, "ANALYSIS_READY.csv")
        print("SUCCESS! Cleaned data saved as ANALYSIS_READY.csv")
    except Exception as e:
        print(f"ERROR: {str(e)}")
