"""
Task 2: Clean the data with Pandas and save as CSV (20 marks)
Cleans the HackerNews JSON data and exports as CSV file
"""

import json
import pandas as pd
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')

def load_json_data(filename="hackernews_data.json"):
    """
    Load JSON data from file
    
    Args:
        filename (str): Input JSON filename
    
    Returns:
        list: List of dictionaries or None if error
    """
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"✓ Loaded {len(data)} records from '{filename}'")
        return data
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found")
        return None
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in '{filename}'")
        return None
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

def clean_hackernews_data(raw_data):
    """
    Clean and preprocess HackerNews data
    
    Args:
        raw_data (list): Raw data from JSON
    
    Returns:
        pd.DataFrame: Cleaned DataFrame
    """
    # Create DataFrame
    df = pd.DataFrame(raw_data)
    
    print("\n" + "="*60)
    print("DATA CLEANING PROCESS")
    print("="*60)
    
    print(f"\nInitial shape: {df.shape}")
    print(f"Columns: {df.columns.tolist()}")
    
    # 1. Handle missing values
    print("\n1. Handling missing values...")
    print(f"   Missing values before:\n{df.isnull().sum()}")
    
    df['title'].fillna('Unknown', inplace=True)
    df['by'].fillna('Unknown', inplace=True)
    df['url'].fillna('No URL', inplace=True)
    df['score'].fillna(0, inplace=True)
    df['descendants'].fillna(0, inplace=True)
    
    print(f"   ✓ Missing values after:\n{df.isnull().sum()}")
    
    # 2. Convert timestamp to readable date
    print("\n2. Converting timestamps to datetime...")
    df['time'] = pd.to_datetime(df['time'], unit='s')
    df['date'] = df['time'].dt.date
    df['hour'] = df['time'].dt.hour
    print("   ✓ Timestamp conversion completed")
    
    # 3. Data type conversions
    print("\n3. Converting data types...")
    df['score'] = df['score'].astype(int)
    df['descendants'] = df['descendants'].astype(int)
    df['id'] = df['id'].astype(int)
    print("   ✓ Data type conversion completed")
    
    # 4. Remove duplicates
    print("\n4. Removing duplicates...")
    initial_rows = len(df)
    df = df.drop_duplicates(subset=['id'])
    removed_rows = initial_rows - len(df)
    print(f"   ✓ Removed {removed_rows} duplicate rows")
    
    # 5. Extract domain from URL (if present)
    print("\n5. Extracting domain from URL...")
    def extract_domain(url):
        try:
            if url.startswith('http'):
                return url.split('/')[2]
            return 'Unknown'
        except:
            return 'Unknown'
    
    df['domain'] = df['url'].apply(extract_domain)
    print("   ✓ Domain extraction completed")
    
    # 6. Reorder columns
    columns_order = ['id', 'title', 'score', 'descendants', 'by', 'date', 'hour', 
                     'type', 'url', 'domain', 'time']
    df = df[[col for col in columns_order if col in df.columns]]
    
    print(f"\n✓ Final shape after cleaning: {df.shape}")
    print(f"✓ Final columns: {df.columns.tolist()}")
    
    return df

def save_to_csv(df, filename="hackernews_cleaned.csv"):
    """
    Save cleaned data to CSV file
    
    Args:
        df (pd.DataFrame): Cleaned DataFrame
        filename (str): Output CSV filename
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        df.to_csv(filename, index=False, encoding='utf-8')
        file_size = len(df)
        print(f"\n✓ Data saved to '{filename}'")
        print(f"  - Total records: {file_size}")
        print(f"  - Columns: {df.shape[1]}")
        return True
    except Exception as e:
        print(f"Error saving to CSV: {e}")
        return False

def display_summary(df):
    """
    Display summary statistics of cleaned data
    
    Args:
        df (pd.DataFrame): Cleaned DataFrame
    """
    print("\n" + "="*60)
    print("DATA SUMMARY")
    print("="*60)
    
    print(f"\nFirst few records:")
    print(df.head())
    
    print(f"\n\nData Info:")
    print(df.info())
    
    print(f"\n\nStatistical Summary (numeric columns):")
    print(df.describe())
    
    print(f"\n\nTop Authors:")
    print(df['by'].value_counts().head(10))
    
    print(f"\n\nTop Domains:")
    print(df['domain'].value_counts().head(10))

def main():
    """Main execution function"""
    print("="*60)
    print("TASK 2: CLEAN DATA WITH PANDAS AND SAVE AS CSV")
    print("="*60)
    
    # Load data
    raw_data = load_json_data("hackernews_data.json")
    if raw_data is None:
        return
    
    # Clean data
    df_cleaned = clean_hackernews_data(raw_data)
    
    # Save to CSV
    save_to_csv(df_cleaned, "hackernews_cleaned.csv")
    
    # Display summary
    display_summary(df_cleaned)
    
    print("\n" + "="*60)
    print("Task 2 completed successfully!")
    print("="*60)

if __name__ == "__main__":
    main()
