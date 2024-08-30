import pandas as pd
from sqlalchemy import create_engine

def csv_to_sql(csv_file, table_name, user, password, host, port, database):
    # Read the CSV file into a Pandas DataFrame
    df = pd.read_csv(csv_file)
    
    # Create the database URL for SQLAlchemy
    database_url = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'
    
    # Create an SQLAlchemy engine
    engine = create_engine(database_url)
    
    # Write the DataFrame to the SQL database
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    
    print(f"Data from {csv_file} has been stored in the '{table_name}' table of the database.")

# Example usage:
csv_file = 'C:\\Users\\Amikar Sinha\\Desktop\\Innovation fair\\Real Policy and Claims Data.csv'  # Replace with your CSV file path
table_name = 'realpolicyclaims'  # Replace with your desired table name
user = 'root'
password = 'admin'
host = 'localhost'
port = 3306
database = 'exception_database'

csv_to_sql(csv_file, table_name, user, password, host, port, database)
