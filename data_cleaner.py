import pandas as pd
import numpy as np
import json
import re
from typing import Dict, List, Union

class QuestionnaireCleaner:
    def __init__(self):
        # Expanded standardization mappings to include common questionnaire phrasing
        self.standard_columns = {
            'timestamp': ['timestamp', 'date', 'time', 'datetime', 'when'],
            'department': [
                'department', 'dept', 'division', 'team',
                'which department are you in', 'what department do you work in',
                'your department'
            ],
            'job_role': [
                'job_role', 'role', 'position', 'job', 'job role',
                'what is your job role', 'your position', 'current role'
            ],
            'ai_tool': [
                'ai_tool_used', 'ai_tool', 'tool', 'ai', 'ai_tool used',
                'what ai tool(s) do you use most', 'primary ai tool',
                'most used ai tool', 'favorite ai tool', 'AI tool(s)'
            ],
            'usage_frequency': [
                'usage_frequency', 'frequency', 'usage', 'how often',
                'how frequently do you use ai tools', 'usage rate',
                'frequency of use'
            ],
            'purpose': [
                'purpose', 'use case', 'application', 'used for',
                'how do you use ai tools', 'primary use of ai',
                'main purpose for using ai'
            ],
            'ease_of_use': [
                'ease_of_use', 'ease', 'usability', 'ease of use',
                'how easy is the tool to use', 'user friendliness',
                'ease rating'
            ],
            'time_saved': [
                'time_saved', 'time', 'efficiency', 'time save', 
                'time saving', 'how much time does it save',
                'time efficiency', 'productivity gain'
            ],
            'suggestions': [
                'improvement_suggestion', 'suggestions', 'feedback', 'comments',
                'what improvements would you suggest', 'feedback for improvement',
                'any suggestions'
            ]
        }
        
        self.suggestion_patterns = {
            'no_suggestion': r'no|none|n/a|not|nothing|nil|nan|null|undefined',
            'training': r'train|guide|tutorial|doc|manual|help|learn|educate',
            'features': r'feature|function|tool|option|capability|improve|enhance|add',
            'integration': r'integrat|connect|api|system|plugin|bridge|import|export',
            'cost': r'cost|price|cheap|afford|license|subscription|fee|pay',
            'accuracy': r'reliable|accurate|quality|precise|correct|better|trust|depend'
        }

    def load_data(self, file_path: str, file_type: str = "CSV") -> pd.DataFrame:
        """Load data from file"""
        try:
            if file_type.upper() == "JSON":
                with open(file_path, 'r') as f:
                    data = json.load(f)
                df = pd.json_normalize(data)
            else:
                df = pd.read_csv(file_path)
            
            if df.empty:
                raise ValueError("Empty file uploaded")
            return df
            
        except Exception as e:
            raise ValueError(f"Error loading file: {str(e)}")

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize the data with proper column renaming"""
        if df is None:
            raise ValueError("No data provided")
            
        df_clean = df.copy()
        df_clean = self._standardize_columns(df_clean)
        
        if 'timestamp' in df_clean.columns:
            df_clean['timestamp'] = pd.to_datetime(df_clean['timestamp'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
        
        if 'department' in df_clean.columns:
            df_clean['department'] = df_clean['department'].fillna('Unknown').str.title()
        
        if 'ai_tool' in df_clean.columns:
            df_clean['ai_tool'] = self._clean_ai_tools(df_clean['ai_tool'])
        
        if 'ease_of_use' in df_clean.columns:
            df_clean['ease_of_use'] = self._clean_ratings(df_clean['ease_of_use'])
        
        if 'time_saved' in df_clean.columns:
            df_clean['time_saved'] = self._clean_ratings(df_clean['time_saved'])
        
        if 'suggestions' in df_clean.columns:
            df_clean['suggestions'] = self._clean_suggestions(df_clean['suggestions'])
            df_clean['suggestion_category'] = self._categorize_suggestions(df_clean['suggestions'])
        
        return df_clean.loc[:, ~df_clean.columns.duplicated()]

    def save_cleaned_data(self, df: pd.DataFrame, output_path: str):
        """Save data with standardized column names"""
        cleaned_df = self.clean_data(df)
        cleaned_df.to_csv(output_path, index=False, encoding='utf-8-sig')

    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Robust column name standardization handling various question formats"""
        column_mapping = {}
        seen_standard = set()
        
        for original_col in df.columns:
            # Normalize the original column name for matching
            col_normalized = str(original_col).lower().strip().replace(' ', '_').replace('(', '').replace(')', '').replace('?', '')
            matched = False
            
            # Check against all possible variations
            for std_col, variations in self.standard_columns.items():
                # Check both direct matches and normalized matches
                variation_patterns = [
                    v.lower().strip().replace(' ', '_').replace('(', '').replace(')', '').replace('?', '')
                    for v in variations
                ]
                
                if any(v == col_normalized for v in variation_patterns):
                    if std_col in seen_standard:
                        # Handle duplicate columns
                        counter = 2
                        new_col = f"{std_col}_{counter}"
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
                # If no match found, keep original but ensure no duplicates
                if original_col in column_mapping.values():
                    counter = 2
                    new_col = f"{original_col}_2"
                    while new_col in column_mapping.values():
                        counter += 1
                        new_col = f"{original_col}_{counter}"
                    column_mapping[original_col] = new_col
                else:
                    column_mapping[original_col] = original_col
        
        return df.rename(columns=column_mapping).loc[:, ~df.columns.duplicated()]

    # [Rest of the helper methods (_clean_ai_tools, _clean_ratings, etc.) remain unchanged]
