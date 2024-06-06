
import pandas as pd
from sqlalchemy import create_engine

def extract(file_path):
    """Extract data from CSV file."""
    return pd.read_csv(file_path)

def transform(data):
    """Transform the data."""
    # Example transformation: filter out rows where age is less than 30
    transformed_data = data[data['age'] >= 30]
    return transformed_data

def load(data, db_url):
    """Load data into the database."""
    engine = create_engine(db_url)
    data.to_sql('users', engine, if_exists='replace', index=False)

def main():
    # File path to the CSV file
    file_path = 'data.csv'
    
    # Database URL (using SQLite for simplicity)
    db_url = 'sqlite:///etl_db.sqlite'
    
    # Extract
    data = extract(file_path)
    
    # Transform
    transformed_data = transform(data)
    
    # Load
    load(transformed_data, db_url)
    
    print("ETL process completed successfully.")

if __name__ == "__main__":
    main()
