import os
import json
import sqlite3
import time
from http.server import HTTPServer, SimpleHTTPRequestHandler
from urllib.parse import urlparse

DB_PATH = "/var/lib/sygnif/swarm.db"
PORT = 8080

class DashboardHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)

        # Serve the HTML frontend
        if parsed.path == "/" or parsed.path == "/index.html":
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            try:
                with open(os.path.join(os.path.dirname(__file__), "dashboard.html"), "rb") as f:
                    self.wfile.write(f.read())
            except FileNotFoundError:
                self.wfile.write(b"<h1>Error: dashboard.html not found</h1>")
            return

        # Serve the API endpoint for events
        if parsed.path == "/api/events":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

            data = []
            if os.path.exists(DB_PATH):
                try:
                    conn = sqlite3.connect(DB_PATH)
                    conn.row_factory = sqlite3.Row
                    cur = conn.cursor()
                    cur.execute('''
                        SELECT created, topic, content, confidence
                        FROM swarm_entries
                        ORDER BY created DESC
                        LIMIT 50
                    ''')
                    rows = cur.fetchall()
                    for r in rows:
                        data.append({
                            "created": r["created"],
                            "topic": r["topic"],
                            "content": r["content"],
                            "confidence": r["confidence"]
                        })
                    conn.close()
                except Exception as e:
                    data = {"error": str(e)}
            else:
                # Return dummy data for testing
                data = [
                    {"created": time.time() - 10, "topic": "xchg.liquidation", "content": '{"side": "sell", "amount": 1500000}', "confidence": 100},
                    {"created": time.time() - 50, "topic": "chain.whale", "content": '{"amount": 500}', "confidence": 95},
                    {"created": time.time() - 100, "topic": "evm.stablecoin_mint", "content": '{"amount": 50000000}', "confidence": 100}
                ]

            self.wfile.write(json.dumps(data).encode("utf-8"))
            return

        # Serve timeseries graph data
        if parsed.path == "/api/graph":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

            # We need timeseries data for the chart. Since swarm.db doesn't have price,
            # we will aggregate liquidations or signal events per minute to simulate a metric graph
            data = []
            if os.path.exists(DB_PATH):
                try:
                    conn = sqlite3.connect(DB_PATH)
                    cur = conn.cursor()
                    # Aggregate event count per minute
                    cur.execute('''
                        SELECT cast(created/60 as int)*60 as minute, COUNT(*) as vol
                        FROM swarm_entries
                        GROUP BY minute
                        ORDER BY minute DESC
                        LIMIT 100
                    ''')
                    rows = cur.fetchall()
                    for r in reversed(rows): # Reverse so oldest is first for chart
                        data.append({"time": r[0], "value": r[1]})
                    conn.close()
                except Exception as e:
                    data = {"error": str(e)}
            else:
                # Provide dummy timeseries
                now = int(time.time())
                for i in range(100, 0, -1):
                    data.append({"time": now - (i*60), "value": 50 + (i % 10) * 5})

            self.wfile.write(json.dumps(data).encode("utf-8"))
            return

        super().do_GET()

def run(server_class=HTTPServer, handler_class=DashboardHandler, port=PORT):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print(f"Starting SYGNIF Dashboard server on port {port}...")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
    print("Server stopped.")

if __name__ == "__main__":
    run()
