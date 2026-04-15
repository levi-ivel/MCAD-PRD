import flask
import duckdb
from flask import render_template

app = flask.Flask(__name__, template_folder='templates', static_folder='templates/static')

VALID_TABLES = ['incidents', 'victims', 'joined', 'filtered', 'aggregated']

@app.route("/")
def home():
    con = duckdb.connect("data/mcad.duckdb")
    try:
        table_names = VALID_TABLES

        # DuckDB Search functionality
        # Convert result rows to dictionary entries
        search_query = flask.request.args.get('q')
        search_results = None
        if search_query:
            try:
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
    # Validate query
    if table_name not in VALID_TABLES:
        return render_template('404.html'), 404
    
    con = duckdb.connect("data/mcad.duckdb")
    try:
        # Get total rows and split into pages, 100 per page
        total = con.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
        per_page = 100
        total_pages = (total + per_page - 1) // per_page

        columns = con.execute("SELECT column_name FROM information_schema.columns WHERE table_name = ?", [table_name]).fetchall()
        col_names = [c[0] for c in columns]
        order_col = 'referenceNumber' if 'referenceNumber' in col_names else col_names[0]

        # Get location of given row_id in the table
        row_id = flask.request.args.get('row_id')
        if row_id:
            count = con.execute(f'SELECT COUNT(*) FROM "{table_name}" WHERE "{order_col}" < ?', [row_id]).fetchone()[0]
            page = (count // per_page) + 1
        else:
            page = flask.request.args.get('page', 1, type=int)

        offset = (page - 1) * per_page
        result = con.execute(f'SELECT * FROM "{table_name}" ORDER BY "{order_col}" LIMIT ? OFFSET ?', [per_page, offset])
        columns = [desc[0] for desc in result.description]
        data = result.fetchall()
        return render_template("table.html", table_name=table_name, columns=columns, data=data, row_count=total, page=page, total_pages=total_pages, per_page=per_page, row_id=row_id)
    except Exception as e:
        return render_template('404.html'), 404
    finally:
        con.close()

@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404

if __name__ == "__main__":
    app.run(debug=True)