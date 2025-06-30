import pandas as pd
import numpy as np
import json
import re
from typing import Dict, List, Union

class QuestionnaireCleaner:
    def __init__(self):
        # Original standardization mappings (unchanged)
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

        # NEW: Mapping for analysis-friendly columns
        self.analysis_columns = {
            'analysis_timestamp': 'timestamp',
            'analysis_department': 'department',
            'analysis_job_role': 'job_role',
            'analysis_ai_tool': 'ai_tool',
            'analysis_usage_frequency': 'usage_frequency',
            'analysis_purpose': 'purpose',
            'analysis_ease_of_use': 'ease_of_use',
            'analysis_time_saved': 'time_saved',
            'analysis_suggestions': 'suggestions'
        }

    # ORIGINAL METHODS REMAIN UNCHANGED (load_data, clean_data, etc.)
    # ... [All your original methods stay exactly the same] ...

    # NEW METHOD: Add analysis-friendly columns
    def add_analysis_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adds new columns with standardized names for analysis tool
        without modifying original data columns
        """
        df_analysis = df.copy()
        
        # First clean the data using existing methods
        df_cleaned = self.clean_data(df_analysis)
        
        # Add new analysis columns
        for analysis_col, std_col in self.analysis_columns.items():
            if std_col in df_cleaned.columns:
                df_analysis[analysis_col] = df_cleaned[std_col]
        
        return df_analysis

    # NEW METHOD: Fixed load_data to properly handle file path
    def load_data(self, file_path: str, file_type: str = "CSV") -> pd.DataFrame:
        """Fixed version that properly handles file paths"""
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
            
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON format")
        except pd.errors.EmptyDataError:
            raise ValueError("Empty CSV file")
        except Exception as e:
            raise ValueError(f"Error loading file: {str(e)}")

    # NEW METHOD: Save with analysis columns
    def save_with_analysis_columns(self, df: pd.DataFrame, output_path: str):
        """Saves data with both original and analysis columns"""
        df_analysis = self.add_analysis_columns(df)
        df_analysis.to_csv(output_path, index=False, encoding='utf-8-sig')


# Example usage
if __name__ == '__main__':
    cleaner = QuestionnaireCleaner()
    
    try:
        # Load original data
        raw_data = cleaner.load_data("cleaned_questionnaire (1).csv")
        
        # Process and save with analysis columns
        cleaner.save_with_analysis_columns(raw_data, "cleaned_questionnaire_ANALYSIS_READY.csv")
        
        print("="*50)
        print("SUCCESS: Data cleaned and saved with analysis columns")
        print("Original columns preserved, added analysis columns:")
        print(list(cleaner.analysis_columns.keys()))
        print("="*50)
        
    except Exception as e:
        print(f"ERROR: {str(e)}")
