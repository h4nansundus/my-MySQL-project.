import mysql.connector
import pandas as pd

# Connect to MySQL database
def connect_to_mysql():
    return mysql.connector.connect(
        host='localhost',
        user='root',
        password='2521SunduS!',
        database='bookstore_pos_system'
    )

# Execute SQL query to extract data
def extract_data_from_mysql():
    # Connect to MySQL
    connection = connect_to_mysql()

    # Write your SQL query here
    query = '''
        SELECT * FROM customers;
    '''

    # Execute the query and fetch results
    cursor = connection.cursor()
    cursor.execute(query)
    data = cursor.fetchall()

    # Get column names
    column_names = [desc[0] for desc in cursor.description]

    # Close the MySQL connection
    connection.close()

    return column_names, data

# Save data to CSV
def save_to_csv(column_names, data, csv_filename):
    df = pd.DataFrame(data, columns=column_names)
    df.to_csv(csv_filename, index=False)
    print(f'Data saved to {csv_filename}')

# Example usage
if __name__ == "__main__":
    # Replace placeholders with your MySQL connection details
    host = 'localhost'
    user = 'root'
    password = '2521SunduS!'
    database = 'bookstore_pos_system'
    table_name = 'customers'
    
    csv_filename = 'customers.csv'  # Specify the desired CSV filename

    # Connect to MySQL and extract data
    connection = mysql.connector.connect(host=host, user=user, password=password, database=database)
    cursor = connection.cursor()

    column_names, data = extract_data_from_mysql()

    # Save data to CSV
    save_to_csv(column_names, data, csv_filename)

    # Close the MySQL connection
    connection.close()
