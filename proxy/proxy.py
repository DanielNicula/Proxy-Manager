import time
import random
from flask import Flask, request, jsonify
import mysql.connector
import os

app = Flask(__name__)

MANAGER_IP = os.getenv("MANAGER_IP")
WORKER_IPS = os.getenv("WORKER_IPS", "").split()
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

LATENCY_THRESHOLD = 0.03   # 30ms threshold, we do chose a random worker below this latency and the lowest ping worker if above

def connect(host):
    return mysql.connector.connect(
        host=host,
        user="root",
        password=MYSQL_PASSWORD,
        database="sakila",
        autocommit=True,
        connection_timeout=2
    )

def measure_latency(host):
    try:
        start = time.time()
        db = connect(host)
        cursor = db.cursor()
        cursor.execute("SELECT 1;")
        cursor.fetchone()
        db.close()
        return time.time() - start
    except Exception:
        return 9999

def is_cluster_under_load(latencies):
    avg_latency = sum(latencies.values()) / len(latencies)
    return avg_latency > LATENCY_THRESHOLD

def select_worker():
    latencies = {ip: measure_latency(ip) for ip in WORKER_IPS}

    # Random Forwarding
    if not is_cluster_under_load(latencies):
        return random.choice(WORKER_IPS)

    # Customized Forwarding
    return min(latencies, key=latencies.get)

def is_read_query(sql):
    return sql.strip().lower().startswith("select")

@app.route("/query", methods=["POST"])
def handle_query():
    data = request.get_json()

    if not data or "sql" not in data:
        return jsonify({"error": "Missing 'sql' field"}), 400

    sql = data["sql"]

    try:
        # We select to which MySql Instance to forward the query
        if is_read_query(sql):
            host = select_worker()
        else:
            # Direct Hit
            host = MANAGER_IP

        db = connect(host)
        cursor = db.cursor(dictionary=True)
        cursor.execute(sql)

        if is_read_query(sql):
            return jsonify(cursor.fetchall())

        return jsonify({"status": "success"})

    except Exception as e:
        return jsonify({"error": str(e)}), 500