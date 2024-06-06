import pandas as pd
from sqlalchemy import create_engine

def extract(file_path):
    """Extract data from CSV file."""
    return pd.read_csv(file_path)

def transform(employee_data, pto_data):
    """Transform and join the PTO data with employee data."""
    joined_data = pd.merge(employee_data, pto_data, how='left', left_on='id', right_on='employee_id')
    return joined_data

def load(data, db_url):
    """Load data into the database."""
    engine = create_engine(db_url)
    data.to_sql('employee_pto', engine, if_exists='replace', index=False)

def main():
    # File paths to the CSV files
    employee_file_path = 'data.csv'
    pto_file_path = 'pto_requests.csv'
    
    # Database URL (using SQLite for simplicity)
    db_url = 'sqlite:///etl_db.sqlite'
    
    # Extract
    employee_data = extract(employee_file_path)
    pto_data = extract(pto_file_path)
    
    # Transform
    joined_data = transform(employee_data, pto_data)
    
    # Load
    load(joined_data, db_url)
    
    print("PTO ETL process completed successfully.")

if __name__ == "__main__":
    main()
