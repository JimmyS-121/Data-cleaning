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
        """Load uploaded file into DataFrame with validation"""
        try:
            if file_type == "JSON":
                data = json.load(file)
                df = pd.json_normalize(data)
            else:
                df = pd.read_csv(file)
            
            # Validate loaded data
            if df.empty:
                raise ValueError("Empty file uploaded")
            return df
            
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON format")
        except pd.errors.EmptyDataError:
            raise ValueError("Empty CSV file")
        except Exception as e:
            raise ValueError(f"Error loading file: {str(e)}")

    def clean_data(self, df):
        """Main data cleaning pipeline with duplicate handling"""
        if df is None:
            raise ValueError("No data provided")
            
        # Create clean copy
        df_clean = df.copy()
        
        # Standardize columns (handles duplicates)
        df_clean = self._standardize_columns(df_clean)
        
        # Clean specific fields
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
        
        return df_clean.dropna(how='all', axis=1)

    def _standardize_columns(self, df):
        """Handle duplicate columns during standardization"""
        column_mapping = {}
        seen_columns = set()
        
        for col in df.columns:
            original_col = col
            col_lower = str(col).lower()
            matched = False
            
            # Check for standard column matches
            for std_col, keywords in self.standard_columns.items():
                if any(kw in col_lower for kw in keywords):
                    # Handle duplicates
                    if std_col in seen_columns:
                        new_col = f"{std_col}_{original_col}"
                    else:
                        new_col = std_col
                    column_mapping[original_col] = new_col
                    seen_columns.add(std_col)
                    matched = True
                    break
                    
            if not matched:
                # Handle duplicate original columns
                if col in seen_columns:
                    new_col = f"{col}_duplicate"
                else:
                    new_col = col
                column_mapping[original_col] = new_col
                seen_columns.add(col)
                
        return df.rename(columns=column_mapping)

    def _clean_ai_tools(self, series):
        """Standardize AI tool names with case handling"""
        tool_map = {
            r'chat.?gpt': 'ChatGPT',
            r'gpt-?4': 'ChatGPT-4',
            r'google.?bard': 'Google Bard',
            r'gemini': 'Google Gemini',
            r'github.?copilot': 'GitHub Copilot',
            r'mid.?journey': 'Midjourney'
        }
        
        series = (
            series.astype(str)
            .str.strip()
            .str.lower()
        )
        
        for pattern, replacement in tool_map.items():
            series = series.str.replace(
                pattern, 
                replacement, 
                case=False, 
                regex=True
            )
            
        return series.str.title()

    def _clean_ratings(self, series, scale=5):
        """Validate and normalize rating columns"""
        series = pd.to_numeric(series, errors='coerce')
        
        # Handle different scales (e.g., 0-4 → 1-5)
        if series.max() == 4 and series.min() == 0:
            series = series + 1
            
        return series.clip(1, scale)
