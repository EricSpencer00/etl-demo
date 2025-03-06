import pandas as pd
import psycopg2
import os

def run_etl():
    # 1. Extract the CSV data
    df = csv_file_path = 'products.csv'
    
    # 2. Transform the data: uppercase the category column
    df['category'] = df['category'].str.upper()

    # 3. Load into PostgreSQL

    host = "localhost"
    dbname = "demo-etl"
    user = "postgres"
    password = "default"
    port = 5432

    try:
        conn = psycopg2.connect(
            host=host,
            dbname=dbname,
            user=user,
            password=password,
            port=port
        )
        conn.autocommit = True
        cur = conn.cursor()

        create_table_query = """
        CREATE TABLE IF NOT EXISTS products (
            product_id      SERIAL PRIMARY KEY,
            product_name    VARCHAR(255),
            price           NUMERIC(10,2),
            category        VARCHAR(255)
        );
        """
        
        # To clear the table each load
        cur.execute("TRUNCATE TABLE products;")

        insert_query = """
            INSERT INTO products (product_id, product_name, price, category)
            VALUES (%s, %s, %s, %s)
        """

        for index, row in df.iterrows():
            # If the product id is in the CSV, use row['product_id']
            # If relying on SERIAL, pass NONE or skip the product id in the column
            data_tuple = (
                row['product_id'],
                row['product_name'],
                row['price'],
                row['category']
            )
            cur.execute(insert_query, data_tuple)

        cur.close()
        conn.close()
        print("ETL process completed")
    except Exception as e:
        print("Error during ETL: ", e)

if __name__ == "__main__":
    run_etl()