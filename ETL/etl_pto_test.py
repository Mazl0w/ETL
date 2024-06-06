import pandas as pd
from sqlalchemy import create_engine

# Database URL
db_url = 'sqlite:///etl_db.sqlite'

# Create a database engine
engine = create_engine(db_url)

# Query the joined data
df = pd.read_sql('SELECT * FROM employee_pto', engine)
print(df)
