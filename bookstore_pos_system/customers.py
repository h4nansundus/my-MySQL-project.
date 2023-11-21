import mysql.connector
import pandas as pd

def connect_to_mysql():
    return mysql.connector.connect(
        host='localhost',
        user='root',
        password='2521SunduS!',
        database='bookstore_pos_system'
    )

def extract_data_from_mysql():
    connection = connect_to_mysql()

    query = '''
        SELECT * FROM customers;
    '''

    cursor = connection.cursor()
    cursor.execute(query)
    data = cursor.fetchall()

    column_names = [desc[0] for desc in cursor.description]

    connection.close()

    return column_names, data

def save_to_csv(column_names, data, csv_filename):
    df = pd.DataFrame(data, columns=column_names)
    df.to_csv(csv_filename, index=False)
    print(f'Data saved to {csv_filename}')

if __name__ == "__main__":
    host = 'localhost'
    user = 'root'
    password = '2521SunduS!'
    database = 'bookstore_pos_system'
    table_name = 'customers'
    
    csv_filename = 'customers.csv' 

    connection = mysql.connector.connect(host=host, user=user, password=password, database=database)
    cursor = connection.cursor()

    column_names, data = extract_data_from_mysql()

    save_to_csv(column_names, data, csv_filename)

    connection.close()
