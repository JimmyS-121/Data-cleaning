import pandas as pd
import numpy as np
import json
import re
from typing import Dict, List, Union

class QuestionnaireCleaner:
    def __init__(self):
        # Column name standardization (matches your CSV headers)
        self.standard_columns = {
            'timestamp': ['timestamp', 'date', 'time', 'datetime'],
            'department': ['which department are you in?', 'department', 'dept'],
            'job_role': ['role:', 'job_role', 'role'],
            'ai_tool': ['what ai tool(s) do you use most?', 'ai_tool'],
            'usage_frequency': ['usage of ai tools', 'usage_frequency'],
            'purpose': ['purpose of using ai tools', 'purpose'],
            'ease_of_use': ['ease of use (rating 1-5, 5 = easiest)', 'ease_of_use'],
            'time_saved': ['how efficiency do the ai tools works for your tasks? (5 = most efficient)', 'time_saved'],
            'suggestions': ['any suggestions or improvement for us about the ai tools or our website ( projectfly )?', 'suggestions']
        }

        # AI tool name standardization (critical for analysis)
        self.tool_standardization = {
            r'chat.?gpt': 'ChatGPT',
            r'poe\b': 'Poe',
            r'canav?a': 'Canva',
            r'mid.?journey': 'Midjourney',
            r'deep.?seek': 'Deepseek',
            r'kling.?ai': 'Kling AI',
            r'copilot': 'Copilot',
            r'gamma': 'Gamma',
            r'kl(?:ing)?\s*ai': 'Kling AI',  # Catches "KlingAI" or "Kling AI"
            r'openai': 'ChatGPT',  # Maps OpenAI to ChatGPT
            r'gpt-\d': 'ChatGPT'   # Catches GPT-3/4
        }

        # Suggestion categorization patterns
        self.suggestion_patterns = {
            'no_suggestion': r'no|none|n/a|not|nothing|nil|nan|null|undefined',
            'training': r'train|guide|tutorial|doc|manual|help|learn|educate',
            'features': r'feature|function|tool|option|capability|improve|enhance|add',
            'integration': r'integrat|connect|api|system|plugin|bridge|import|export',
            'cost': r'cost|price|cheap|afford|license|subscription|fee|pay',
            'accuracy': r'reliable|accurate|quality|precise|correct|better|trust|depend'
        }

    def load_data(self, file_path: str, file_type: str = "CSV") -> pd.DataFrame:
        """Load data from CSV or JSON file."""
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

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Main cleaning pipeline with strict AI tool standardization."""
        if df is None:
            raise ValueError("No data provided")
            
        df_clean = df.copy()
        df_clean = self._standardize_columns(df_clean)
        
        # Timestamp standardization
        if 'timestamp' in df_clean.columns:
            df_clean['timestamp'] = pd.to_datetime(
                df_clean['timestamp'],
                errors='coerce'
            ).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Department cleaning
        if 'department' in df_clean.columns:
            df_clean['department'] = (
                df_clean['department']
                .fillna('Unknown')
                .str.title()
            )
        
        # AI Tool standardization (MOST CRITICAL PART)
        if 'ai_tool' in df_clean.columns:
            df_clean['ai_tool'] = self._clean_ai_tools(df_clean['ai_tool'])
        
        # Usage frequency standardization
        if 'usage_frequency' in df_clean.columns:
            df_clean['usage_frequency'] = self._clean_usage_frequency(df_clean['usage_frequency'])
        
        # Rating columns cleaning
        if 'ease_of_use' in df_clean.columns:
            df_clean['ease_of_use'] = self._clean_ratings(df_clean['ease_of_use'])
        
        if 'time_saved' in df_clean.columns:
            df_clean['time_saved'] = self._clean_ratings(df_clean['time_saved'])
        
        # Suggestions cleaning and categorization
        if 'suggestions' in df_clean.columns:
            df_clean['suggestions'] = self._clean_suggestions(df_clean['suggestions'])
            df_clean['suggestion_category'] = self._categorize_suggestions(df_clean['suggestions'])
        
        return df_clean.loc[:, ~df_clean.columns.duplicated()]

    def save_cleaned_data(self, df: pd.DataFrame, output_path: str):
        """Save cleaned data to CSV with UTF-8 encoding."""
        df.to_csv(output_path, index=False, encoding='utf-8-sig')

    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map raw column names to standardized versions."""
        column_mapping = {}
        
        for original_col in df.columns:
            original_lower = str(original_col).lower().strip().replace(' ', '_')
            matched = False
            
            for std_col, variations in self.standard_columns.items():
                for variant in variations:
                    if original_lower == variant.lower().strip().replace(' ', '_'):
                        column_mapping[original_col] = std_col
                        matched = True
                        break
                if matched:
                    break
            
            if not matched:
                column_mapping[original_col] = original_col
        
        return df.rename(columns=column_mapping)

    def _clean_ai_tools(self, series: pd.Series) -> pd.Series:
        """Strict standardization of AI tool names."""
        def _standardize_tool(tool_name: str) -> str:
            if not isinstance(tool_name, str) or tool_name.lower() in ('nan', 'none', ''):
                return 'Unknown'
            
            tool_name = tool_name.lower().strip()
            
            for pattern, standardized_name in self.tool_standardization.items():
                if re.search(pattern, tool_name, re.IGNORECASE):
                    return standardized_name
            
            return tool_name.title()  # Fallback for unrecognized tools
        
        return series.apply(_standardize_tool)

    def _clean_usage_frequency(self, series: pd.Series) -> pd.Series:
        """Standardize usage frequency values."""
        freq_map = {
            'daily': 'Daily',
            'weekly': 'Weekly',
            'monthly': 'Monthly',
            'rarely': 'Rarely',
            'never': 'Never'
        }
        return series.str.lower().map(freq_map).fillna(series.str.title())

    def _clean_suggestions(self, series: pd.Series) -> pd.Series:
        """Clean and format suggestions."""
        cleaned = (
            series
            .astype(str)
            .str.strip()
            .str.lower()
            .replace(r'^\s*$', np.nan, regex=True)
            .replace(r'[^\w\s.,;!?]', '', regex=True)
        )
        
        no_suggestion_pattern = r'^(no|none|n/a|not|nothing|nil|nan|null|undefined)'
        return cleaned.replace(no_suggestion_pattern, 'No suggestions', regex=True)

    def _categorize_suggestions(self, series: pd.Series) -> pd.Series:
        """Categorize suggestions into predefined groups."""
        def _categorize(suggestion: str) -> str:
            if not isinstance(suggestion, str) or suggestion.lower() == 'no suggestions':
                return 'No suggestions'
            
            suggestion = suggestion.lower()
            
            for category, pattern in self.suggestion_patterns.items():
                if re.search(pattern, suggestion, re.I):
                    return category.replace('_', ' ').title()
            
            return 'Other suggestions'
        
        return series.apply(_categorize)

    def _clean_ratings(self, series: pd.Series, scale: int = 5) -> pd.Series:
        """Clean and normalize rating columns."""
        series = pd.to_numeric(series, errors='coerce')
        return series.clip(1, scale)


if __name__ == '__main__':
    # Example usage
    cleaner = QuestionnaireCleaner()
    
    # Load raw data
    raw_data = cleaner.load_data("cleaned_questionnaire (1).csv", "CSV")
    
    # Clean data (now with strict AI tool standardization)
    cleaned_data = cleaner.clean_data(raw_data)
    
    # Save cleaned data
    cleaner.save_cleaned_data(cleaned_data, "cleaned_questionnaire_ANALYSIS_READY.csv")
    
    print("="*50)
    print("Cleaning complete! Output saved to 'cleaned_questionnaire_ANALYSIS_READY.csv'")
    print("Unique AI tools detected:", cleaned_data['ai_tool'].unique())
    print("="*50)
