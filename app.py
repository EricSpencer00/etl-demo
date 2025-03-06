from flask import Flask, render_template
import psycopg2

app = Flask(__name__)

@app.route("/")
def home():
    return "Welcome to the ETL!"

@app.route("/products")
def show_products():
    host = "localhost"
    dbname = "demo-etl"
    user = "ericspencer"
    password = "default"
    port = 5432

    try:
        conn = psycopg2.connect(
            host = host,
            dbname = dbname,
            user = user,
            password = password,
            port = port
        )
        cur = conn.cursor()

        # This will select columns with their data in the table
        select_query = "SELECT product_id, product_name, price, category FROM products;"
        # executes query
        cur.execute(select_query)
        # This will set the rows variable to the cursors set
        rows = cur.fetchall()

        products = []
        for row in rows:
            product_dict = {
                'product_id': row[0],
                'product_name': row[1],
                'price': row[2],
                'category': row[3]
            }
            products.append(product_dict)

        cur.close()
        conn.close()

        return render_template("products.html", products=products)
    
    except Exception as e:
        return f"Error connecting to DB: {str(e)}"
    
if __name__ == "__main__":
    app.run(debug=True)