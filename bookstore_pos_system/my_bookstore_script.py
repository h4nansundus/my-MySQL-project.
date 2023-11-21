import pandas as pd
import mysql.connector
import os

# Connect to MySQL database
def connect_to_mysql():
    return mysql.connector.connect(
        host='localhost',
        user='root',
        password='2521SunduS!',
        database='bookstore_pos_system'
    )

# Ingest data into MySQL
def ingest_data_into_mysql():
    # Modify file paths
    customers_path = r'path_to_customers.csv'
    invoices_path = r'path_to_invoices.csv'
    invoices_lines_path = r'path_to_invoices_lines.csv'

    # Check if CSV files exist
    if not all(os.path.exists(file_path) for file_path in [customers_path, invoices_path, invoices_lines_path]):
        print("One or more CSV files do not exist.")
        return

    # Read CSV files
    customers_df = pd.read_csv(customers_path)
    invoices_df = pd.read_csv(invoices_path)
    invoices_lines_df = pd.read_csv(invoices_lines_path)

    # Connect to MySQL
    connection = connect_to_mysql()

    # Write DataFrames to MySQL database
    customers_df.to_sql('customers', connection, index=False, if_exists='replace')
    invoices_df.to_sql('invoices', connection, index=False, if_exists='replace')
    invoices_lines_df.to_sql('invoices_lines', connection, index=False, if_exists='replace')

    # Close the MySQL connection
    connection.close()

# Execute SQL queries
def execute_sql_queries():
    # Connect to MySQL
    connection = connect_to_mysql()

    # Query 1: Write your SQL query here
    query1 = '''
        -- Your SQL query for Query 1
    '''

    result1 = pd.read_sql_query(query1, connection)
    print("Query 1 Result:")
    print(result1)

    # Query 2: Write your SQL query here
    query2 = '''
        -- Your SQL query for Query 2
    '''

    result2 = pd.read_sql_query(query2, connection)
    print("\nQuery 2 Result:")
    print(result2)

    # Query 3: Write your SQL query here
    query3 = '''
        -- Your SQL query for Query 3
    '''

    result3 = pd.read_sql_query(query3, connection)
    print("\nQuery 3 Result:")
    print(result3)

    # Close the MySQL connection
    connection.close()

# Ingest data into MySQL
ingest_data_into_mysql()

# Execute SQL queries
execute_sql_queries()
