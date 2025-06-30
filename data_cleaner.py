import pandas as pd
import numpy as np
import json
import re
from typing import Dict, List, Union

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
        
        self.suggestion_patterns = {
            'no_suggestion': r'no|none|n/a|not|nothing|nil|nan|null|undefined',
            'training': r'train|guide|tutorial|doc|manual|help|learn|educate',
            'features': r'feature|function|tool|option|capability|improve|enhance|add',
            'integration': r'integrat|connect|api|system|plugin|bridge|import|export',
            'cost': r'cost|price|cheap|afford|license|subscription|fee|pay',
            'accuracy': r'reliable|accurate|quality|precise|correct|better|trust|depend'
        }

    def load_data(self, file, file_type: str) -> pd.DataFrame:
        try:
            if file_type == "JSON":
                data = json.load(file)
                df = pd.json_normalize(data)
            else:
                df = pd.read_csv(file)
            
            if df.empty:
                raise ValueError("Empty file uploaded")
            return df
            
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON format")
        except pd.errors.EmptyDataError:
            raise ValueError("Empty CSV file")
        except Exception as e:
            raise ValueError(f"Error loading file: {str(e)}")

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None:
            raise ValueError("No data provided")
            
        df_clean = df.copy()
        df_clean = self._standardize_columns(df_clean)
        
        # Standard cleaning for all columns
        if 'timestamp' in df_clean.columns:
            df_clean['timestamp'] = pd.to_datetime(
                df_clean['timestamp'],
                errors='coerce'
            ).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        if 'department' in df_clean.columns:
            df_clean['department'] = (
                df_clean['department']
                .fillna('Unknown')
                .str.title()
            )
        
        if 'ai_tool' in df_clean.columns:
            df_clean['ai_tool'] = self._clean_ai_tools(df_clean['ai_tool'])
        
        if 'ease_of_use' in df_clean.columns:
            df_clean['ease_of_use'] = self._clean_ratings(df_clean['ease_of_use'])
        
        if 'time_saved' in df_clean.columns:
            df_clean['time_saved'] = self._clean_ratings(df_clean['time_saved'])
        
        if 'suggestions' in df_clean.columns:
            df_clean['suggestions'] = self._clean_suggestions(df_clean['suggestions'])
            df_clean['suggestion_category'] = self._categorize_suggestions(df_clean['suggestions'])
        
        # Ensure no duplicate columns remain
        df_clean = df_clean.loc[:, ~df_clean.columns.duplicated()]
        return df_clean.dropna(how='all', axis=1)

    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        column_mapping = {}
        seen_standard = set()
        
        for original_col in df.columns:
            col_lower = str(original_col).lower().replace(' ', '_')
            matched = False
            
            for std_col, variations in self.standard_columns.items():
                if any(v.lower().replace(' ', '_') == col_lower for v in variations):
                    if std_col in seen_standard:
                        # Handle duplicate standard columns
                        new_col = f"{std_col}_2"
                        counter = 2
                        while new_col in column_mapping.values():
                            counter += 1
                            new_col = f"{std_col}_{counter}"
                    else:
                        new_col = std_col
                        seen_standard.add(std_col)
                    
                    column_mapping[original_col] = new_col
                    matched = True
                    break
            
            if not matched:
                # Handle non-standard columns
                if original_col in column_mapping.values():
                    counter = 2
                    new_col = f"{original_col}_2"
                    while new_col in column_mapping.values():
                        counter += 1
                        new_col = f"{original_col}_{counter}"
                    column_mapping[original_col] = new_col
                else:
                    column_mapping[original_col] = original_col
        
        # Apply mapping and ensure no duplicates
        df = df.rename(columns=column_mapping)
        return df.loc[:, ~df.columns.duplicated()]

    def _clean_suggestions(self, series: pd.Series) -> pd.Series:
        cleaned = (
            series
            .astype(str)
            .str.strip()
            .str.lower()
            .replace(r'^\s*$', np.nan, regex=True)
        )
        
        no_suggestion_pattern = r'^(no|none|n/a|not|nothing|nil|nan|null|undefined)'
        cleaned = cleaned.replace(no_suggestion_pattern, 'No suggestions', regex=True)
        cleaned = cleaned.str.replace(r'[^\w\s.,;!?]', '', regex=True)
        cleaned = cleaned.str.replace(r'\s+', ' ', regex=True)
        
        return cleaned.str.title()

    def _categorize_suggestions(self, series: pd.Series) -> pd.Series:
        def _categorize(suggestion: str) -> str:
            if not isinstance(suggestion, str):
                return 'Uncategorized'
            
            suggestion = suggestion.lower()
            
            if re.search(self.suggestion_patterns['no_suggestion'], suggestion, re.I):
                return 'No suggestions'
            elif re.search(self.suggestion_patterns['training'], suggestion, re.I):
                return 'Training/guidance'
            elif re.search(self.suggestion_patterns['features'], suggestion, re.I):
                return 'Feature improvements'
            elif re.search(self.suggestion_patterns['integration'], suggestion, re.I):
                return 'Better integration'
            elif re.search(self.suggestion_patterns['cost'], suggestion, re.I):
                return 'Cost reduction'
            elif re.search(self.suggestion_patterns['accuracy'], suggestion, re.I):
                return 'Improved accuracy'
            else:
                return 'Other suggestions'
        
        return series.apply(_categorize)

    def _clean_ai_tools(self, series: pd.Series) -> pd.Series:
        tool_map = {
            r'chat.?gpt': 'ChatGPT',
            r'gpt-?4': 'ChatGPT-4',
            r'google.?bard': 'Google Bard',
            r'gemini': 'Google Gemini',
            r'github.?copilot': 'GitHub Copilot',
            r'mid.?journey': 'Midjourney'
        }
        
        series = series.astype(str).str.strip().str.lower()
        
        for pattern, replacement in tool_map.items():
            series = series.str.replace(pattern, replacement, case=False, regex=True)
            
        return series.str.title()

    def _clean_ratings(self, series: pd.Series, scale: int = 5) -> pd.Series:
        series = pd.to_numeric(series, errors='coerce')
        if series.max() == 4 and series.min() == 0:
            series = series + 1
        return series.clip(1, scale)
