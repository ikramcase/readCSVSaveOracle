import pandas as pd
import cx_Oracle

# Define the file path
file_path = './transactions.csv'

# Define the Oracle database connection details
oracle_username = 'your_username'
oracle_password = 'your_password'
oracle_host = 'your_host'
oracle_port = 'your_port'
oracle_service_name = 'your_service_name'

# Read the CSV file into a pandas DataFrame
df = pd.read_csv(file_path)

# Extract the required columns
columns_to_extract = ['Account Number', 'Balance', 'Credit', 'Debit', 'Transaction Description', 'Date']
extracted_data = df[columns_to_extract]

# Establish a connection to Oracle
oracle_dsn = cx_Oracle.makedsn(oracle_host, oracle_port, service_name=oracle_service_name)
oracle_connection = cx_Oracle.connect(oracle_username, oracle_password, dsn=oracle_dsn)

# Create a cursor to execute SQL statements
cursor = oracle_connection.cursor()

# Create a table to store the extracted data in Oracle
table_name = 'transactions'
create_table_query = f"CREATE TABLE {table_name} (Account_Number VARCHAR2(50), Balance NUMBER, Credit NUMBER, Debit NUMBER, Transaction_Description VARCHAR2(200), Transaction_Date DATE)"
cursor.execute(create_table_query)

# Insert the extracted data into the Oracle table
insert_query = f"INSERT INTO {table_name} VALUES (:1, :2, :3, :4, :5, TO_DATE(:6, 'YYYY-MM-DD'))"
cursor.executemany(insert_query, extracted_data.itertuples(index=False))

# Commit the changes to the database
oracle_connection.commit()

# Close the cursor and the database connection
cursor.close()
oracle_connection.close()
