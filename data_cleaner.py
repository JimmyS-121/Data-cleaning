import pandas as pd
import numpy as np
import json
import re
from typing import Optional, Union

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
        
        return df_clean.dropna(how='all', axis=1)

    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        column_mapping = {}
        seen_columns = set()
        
        for col in df.columns:
            original_col = col
            col_lower = str(col).lower().replace(' ', '_')
            matched = False
            
            for std_col, keywords in self.standard_columns.items():
                if any(kw in col_lower for kw in [k.lower().replace(' ', '_') for k in keywords]):
                    if std_col in seen_columns and not (std_col == 'timestamp' and 'time' in col_lower):
                        new_col = f"{std_col}_duplicate_{len([c for c in column_mapping.values() if c.startswith(std_col)])}"
                    else:
                        new_col = std_col
                    column_mapping[original_col] = new_col
                    seen_columns.add(std_col)
                    matched = True
                    break
                    
            if not matched:
                if col in seen_columns:
                    new_col = f"{col}_duplicate"
                else:
                    new_col = col
                column_mapping[original_col] = new_col
                seen_columns.add(col)
                
        return df.rename(columns=column_mapping)

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
