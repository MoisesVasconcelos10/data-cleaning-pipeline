import pandas as pd

def load_data(filepath):
          return pd.read_csv(filepath)

def remove_duplicates(df):
          before = len(df)
          df = df.drop_duplicates()
          print(f"Removed {before - len(df)} duplicate rows")
          return df

def handle_missing(df, strategy="drop"):
          if strategy == "drop":
                        df = df.dropna()
elif strategy == "fill":
        df = df.fillna(0)
    return df

def normalize_columns(df):
          df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
          return df

def run_pipeline(filepath, output="cleaned_data.csv"):
          print("Starting data cleaning pipeline...")
          df = load_data(filepath)
          df = remove_duplicates(df)
          df = handle_missing(df)
          df = normalize_columns(df)
          df.to_csv(output, index=False)
          print(f"Pipeline complete. Clean data saved to {output}")
          return df

if __name__ == "__main__":
          print("Data Cleaning Pipeline initialized.")
          print("Usage: run_pipeline('your_file.csv')")
      
