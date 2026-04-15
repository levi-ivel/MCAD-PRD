import flask
import duckdb
from flask import render_template

app = flask.Flask(__name__, template_folder='templates', static_folder='templates/static')

@app.route("/")
def home():
    con = duckdb.connect("data/mcad.duckdb")
    try:
        # Alle tables die we hebben
        table_names = ['incidents', 'victims', 'joined', 'filtered', 'aggregated']
        
        # DuckDB Search functionaliteit
        # Zet resultaten rows om naar dictionary entries
        search_query = flask.request.args.get('q')
        search_results = None
        if search_query:
            try:
                con.execute("INSTALL fts")
                con.execute("LOAD fts")
                query = """
                    SELECT referenceNumber, title, method, year, viccountry, area,
                           fts_main_joined.match_bm25(referenceNumber, ?) AS score 
                    FROM joined 
                    WHERE score IS NOT NULL 
                    ORDER BY score DESC 
                    LIMIT 25
                """
                res = con.execute(query, [search_query]).fetchall()
                
                search_results = []
                for row in res:
                    search_results.append({
                        'id': row[0],
                        'title': row[1],
                        'method': row[2],
                        'year': row[3],
                        'country': row[4],
                        'area': row[5]
                    })
            except Exception as e:
                app.logger.error(f"FTS Search Error: {e}")
                search_results = []

        return render_template("home.html", table_names=table_names, search_results=search_results, search_query=search_query)
    except Exception as e:
        return f"Error: {str(e)}", 500
    finally:
        con.close()

@app.route("/table/<table_name>")
def show_table(table_name):
    con = duckdb.connect("data/mcad.duckdb")
    try:
        # Haal info uit gegeven table
        result = con.execute(f'SELECT * FROM "{table_name}"')
        columns = [desc[0] for desc in result.description]
        data = result.fetchall()
        row_count = len(data)
        return render_template("table.html", table_name=table_name, columns=columns, data=data, row_count=row_count)
    except Exception as e:
        return render_template('404.html'), 404
    finally:
        con.close()

@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404

if __name__ == "__main__":
    app.run(debug=True)
