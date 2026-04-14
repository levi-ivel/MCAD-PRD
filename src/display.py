import flask
import duckdb
from flask import render_template

app = flask.Flask(__name__, template_folder='templates', static_folder='templates/static')

@app.route("/")
def home():
    con = duckdb.connect("data/mcad.duckdb")
    try:
        # Haal alle tables op
        tables = con.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]
        
        # Custom volgorde voor weergave
        order_priority = {
            'incidents': 1,
            'victims': 2,
            'joined': 3,
            'filtered': 4,
            'aggregated': 5
        }
        table_names.sort(key=lambda x: order_priority.get(x, 999))
        return render_template("home.html", table_names=table_names)
    except Exception as e:
        return f"Error: {str(e)}", 500
    finally:
        con.close()

@app.route("/table/<table_name>")
def show_table(table_name):
    con = duckdb.connect("data/mcad.duckdb")
    try:
        # Check of gegeven table bestaat
        tables = con.execute("SHOW TABLES").fetchall()
        valid_tables = [t[0] for t in tables]
        if table_name not in valid_tables:
            return f"Tabel '{table_name}' niet gevonden", 404
        
        # Haal info uit gegeven table
        result = con.execute(f'SELECT * FROM "{table_name}"')
        columns = [desc[0] for desc in result.description]
        data = result.fetchall()
        return render_template("table.html", table_name=table_name, columns=columns, data=data)
    except Exception as e:
        return f"Error: {str(e)}", 500
    finally:
        con.close()

if __name__ == "__main__":
    app.run(debug=True)
