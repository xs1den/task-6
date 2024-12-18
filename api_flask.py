from flask import Flask, request, jsonify
import psycopg2
import os
from dotenv import load_dotenv


load_dotenv()

app = Flask(__name__)


host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
dbname = os.getenv("DB_NAME")


conn = psycopg2.connect(
    host=host,
    port=port,
    user=user,
    password=password,
    dbname=dbname,
    sslmode="require"
)
cur = conn.cursor()

@app.route('/setup', methods=['POST'])
def setup_table():
    cur.execute("DROP TABLE IF EXISTS test_perf;")
    cur.execute("""
        CREATE TABLE test_perf (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50),
            value INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    cur.execute("CREATE INDEX idx_name ON test_perf(name);")
    cur.execute("CREATE INDEX idx_value ON test_perf(value);")
    conn.commit()
    return jsonify({"message": "Table created successfully with indexes"})

@app.route('/insert', methods=['POST'])
def insert_data():
    data = request.json['data']
    args_str = ",".join(cur.mogrify("(%s, %s)", (row['name'], row['value'])).decode() for row in data)
    cur.execute("INSERT INTO test_perf (name, value) VALUES " + args_str)
    conn.commit()
    return jsonify({"message": "Data inserted successfully"})

@app.route('/select', methods=['GET'])
def select_data():
    cur.execute("EXPLAIN ANALYZE SELECT * FROM test_perf;")
    plan = cur.fetchall()
    cur.execute("SELECT * FROM test_perf;")
    rows = cur.fetchall()
    return jsonify({"execution_plan": plan, "data": rows})

@app.route('/update', methods=['PUT'])
def update_data():
    cur.execute("EXPLAIN ANALYZE UPDATE test_perf SET value = value + 1;")
    plan = cur.fetchall()
    cur.execute("UPDATE test_perf SET value = value + 1;")
    conn.commit()
    return jsonify({"execution_plan": plan, "message": "Data updated successfully"})

@app.route('/delete', methods=['DELETE'])
def delete_data():
    cur.execute("EXPLAIN ANALYZE DELETE FROM test_perf;")
    plan = cur.fetchall()
    cur.execute("DELETE FROM test_perf;")
    conn.commit()
    return jsonify({"execution_plan": plan, "message": "Data deleted successfully"})

if __name__ == '__main__':
    app.run(debug=True)
