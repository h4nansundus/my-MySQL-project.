import sqlalchemy
from sqlalchemy import text

username = 'root'
password = '2521SunduS!'
host = 'localhost'
port = '3306'  
database_name = 'bookstore_pos_system'

connection_string = f'mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database_name}'

engine = sqlalchemy.create_engine(connection_string)

connection = engine.connect()

schema_queries = [
    """
    CREATE TABLE IF NOT EXISTS `bookstore_pos_system`.`customer` (
      `id` INT NOT NULL,
      `name` VARCHAR(45) NOT NULL,
      `email` VARCHAR(255) NULL,
      `tel` VARCHAR(45) NULL,
      `created_at` TIMESTAMP NOT NULL,
      `updated_at` TIMESTAMP NOT NULL,
      PRIMARY KEY (`id`),
      UNIQUE INDEX `email_UNIQUE` (`email` ASC) VISIBLE,
      UNIQUE INDEX `tel_UNIQUE` (`tel` ASC) VISIBLE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS `bookstore_pos_system`.`invoices` (
      `id` INT NOT NULL,
      `number` INT NOT NULL,
      `sub_total` DECIMAL(10,2) NOT NULL,
      `tax_total` DECIMAL(10,2) NOT NULL,
      `total` DECIMAL(10,2) NOT NULL,
      `customer_id` INT NOT NULL,
      `created_at` TIMESTAMP NOT NULL,
      `updated_at` TIMESTAMP NOT NULL,
      PRIMARY KEY (`id`)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS `bookstore_pos_system`.`invoices_lines` (
      `id` INT NOT NULL,
      `description` VARCHAR(45) NOT NULL,
      `unit_price` DECIMAL(10,2) NOT NULL,
      `quantity` INT NOT NULL,
      `sub_total` DECIMAL(10,2) NOT NULL,
      `tax_total` DECIMAL(10,2) NOT NULL,
      `total` DECIMAL(10,2) NOT NULL,
      `tax_id` VARCHAR(45) NOT NULL,
      `sku_id` INT NOT NULL,
      `invoice_id` INT NOT NULL,
      PRIMARY KEY (`id`)
    );
    """
]

for query in schema_queries:
    connection.execute(text(query))

query_b = """
    SELECT
        customers.id,
        customers.name,
        SUM(invoices_lines.quantity) AS total_books_purchased
    FROM
        customers
    JOIN invoices ON customers.id = invoices.customer_id
    JOIN invoices_lines ON invoices.id = invoices_lines.invoice_id
    GROUP BY
        customers.id
    HAVING
        total_books_purchased > 5;
"""

result_b = connection.execute(text(query_b))

print("\nResult for b. Number of customers purchasing more than 5 books:")
for row in result_b:
    print(row)

query_c = """
    SELECT
        customers.id,
        customers.name
    FROM
        customers
    LEFT JOIN
        invoices ON customers.id = invoices.customer_id
    WHERE
        invoices.id IS NULL;
"""

result_c = connection.execute(text(query_c))

print("\nResult for c. List of customers who never purchased anything:")
for row in result_c:
    print(row)

query_d = """
    SELECT
        customers.id AS customer_id,
        customers.name AS customer_name,
        invoices_lines.description AS book_description,
        invoices_lines.quantity AS book_quantity
    FROM
        customers
    JOIN
        invoices ON customers.id = invoices.customer_id
    JOIN
        invoices_lines ON invoices.id = invoices_lines.invoice_id;
"""

result_d = connection.execute(text(query_d))

print("\nResult for d. List of books purchased with the users:")
for row in result_d:
    print(row)

connection.close()
