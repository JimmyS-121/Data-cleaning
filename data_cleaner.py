import pandas as pd
import numpy as np
import re
from typing import Dict

class QuestionnaireCleaner:
    def __init__(self):
        # Preserve original headers while standardizing internally
        self.original_headers = {
            'timestamp': 'Timestamp',
            'department': 'Which Department are you in?',
            'job_role': 'Role:',
            'ai_tool': 'What AI tool(s) do you use most? (e.g. Poe/ChatGPT)',
            'usage_frequency': 'Usage of AI Tools',
            'purpose': 'Purpose of using AI tools',
            'ease_of_use': 'Ease of use (Rating 1-5, 5 = easiest)',
            'time_saved': 'How efficiency do the AI tools works for your tasks? (5 = most efficient)',
            'suggestions': 'Any suggestions or improvement for us about the AI tools or our website (ProjectFly)?'
        }

        # AI tool standardization mapping
        self.tool_standardization = {
            r'chat.?gpt': 'ChatGPT',
            r'poe\b': 'Poe',
            r'canav?a': 'Canva',
            r'mid.?journey': 'Midjourney',
            r'deep.?seek': 'Deepseek',
            r'kling.?ai': 'Kling AI',
            r'copilot': 'Copilot',
            r'gamma': 'Gamma'
        }

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Main cleaning pipeline that preserves original headers"""
        df_clean = df.copy()
        
        # Standardize data while keeping original column names
        if self.original_headers['ai_tool'] in df_clean.columns:
            df_clean[self.original_headers['ai_tool']] = self._clean_ai_tools(
                df_clean[self.original_headers['ai_tool']]
            )
        
        if self.original_headers['usage_frequency'] in df_clean.columns:
            df_clean[self.original_headers['usage_frequency']] = self._clean_usage_frequency(
                df_clean[self.original_headers['usage_frequency']]
            )
        
        if self.original_headers['ease_of_use'] in df_clean.columns:
            df_clean[self.original_headers['ease_of_use']] = self._clean_ratings(
                df_clean[self.original_headers['ease_of_use']]
            )
        
        if self.original_headers['time_saved'] in df_clean.columns:
            df_clean[self.original_headers['time_saved']] = self._clean_ratings(
                df_clean[self.original_headers['time_saved']]
            )
        
        if self.original_headers['suggestions'] in df_clean.columns:
            df_clean[self.original_headers['suggestions']] = self._clean_suggestions(
                df_clean[self.original_headers['suggestions']]
            )
        
        return df_clean

    def _clean_ai_tools(self, series: pd.Series) -> pd.Series:
        """Standardize AI tool names while preserving original column header"""
        def standardize_tool(tool_name: str) -> str:
            if pd.isna(tool_name):
                return 'Unknown'
            
            tool_name = str(tool_name).lower().strip()
            for pattern, standardized_name in self.tool_standardization.items():
                if re.search(pattern, tool_name, re.IGNORECASE):
                    return standardized_name
            return tool_name.title()
        
        return series.apply(standardize_tool)

    def _clean_usage_frequency(self, series: pd.Series) -> pd.Series:
        """Clean usage frequency values"""
        freq_map = {
            'daily': 'Daily',
            'weekly': 'Weekly',
            'monthly': 'Monthly',
            'rarely': 'Rarely'
        }
        return series.str.lower().map(freq_map).fillna(series.str.title())

    def _clean_suggestions(self, series: pd.Series) -> pd.Series:
        """Clean suggestions while preserving original header"""
        return series.str.strip().replace(
            r'^(no|none|n/a|not|nothing|nil|nan|null|undefined)$', 
            'No suggestions', 
            regex=True
        )

    def _clean_ratings(self, series: pd.Series) -> pd.Series:
        """Ensure ratings are numeric between 1-5"""
        return pd.to_numeric(series, errors='coerce').clip(1, 5)

    def save_cleaned_data(self, df: pd.DataFrame, output_path: str):
        """Save data with original recognizable headers"""
        df.to_csv(output_path, index=False, encoding='utf-8-sig')

if __name__ == '__main__':
    cleaner = QuestionnaireCleaner()
    
    # Load your original data
    raw_data = pd.read_csv("cleaned_questionnaire (1).csv")
    
    # Clean data while preserving headers
    cleaned_data = cleaner.clean_data(raw_data)
    
    # Save with original recognizable headers
    cleaner.save_cleaned_data(cleaned_data, "cleaned_questionnaire_ANALYSIS_READY.csv")
    
    print("Cleaning complete! Output saved with original recognizable headers.")
    print("Sample cleaned AI tools:", cleaned_data[cleaner.original_headers['ai_tool']].unique())
