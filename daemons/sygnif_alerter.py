import os
import sys
import time
import json
import sqlite3
import requests

DB_PATH = "/var/lib/sygnif/swarm.db"
STATE_FILE = "/var/lib/sygnif/alerter_state.json"

def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading state: {e}", file=sys.stderr)
    return {"last_id": None}

def save_state(state):
    try:
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        with open(STATE_FILE + ".tmp", "w") as f:
            json.dump(state, f)
        os.replace(STATE_FILE + ".tmp", STATE_FILE)
    except Exception as e:
        print(f"Error saving state: {e}", file=sys.stderr)

def get_db_connection():
    try:
        return sqlite3.connect(DB_PATH)
    except Exception as e:
        print(f"Database connection error: {e}", file=sys.stderr)
        return None

def check_trigger_rules(topic, meta):
    if topic == "xchg.liquidation_cluster" and meta.get("value_usd", 0) > 500_000:
        return True
    if topic == "chain.mempool_whale" and meta.get("value_btc", 0) > 100:
        return True
    if topic and str(topic).startswith("norm.") and (meta.get("zscore", 0) > 2.5 or meta.get("zscore", 0) < -2.5):
        return True
    if topic == "chart.indicator" and (meta.get("percentile", 50) >= 95 or meta.get("percentile", 50) <= 5):
        return True
    if topic == "chain.dormancy_break":
        return True
    return False

def main():
    state = load_state()

    conn = get_db_connection()
    if conn and state.get("last_id") is None:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(rowid) FROM swarm_entries")
            row = cursor.fetchone()
            if row and row[0] is not None:
                state["last_id"] = row[0]
            else:
                state["last_id"] = 0
            save_state(state)
        except Exception as e:
            print(f"Error initializing last_id: {e}", file=sys.stderr)
            state["last_id"] = 0
        finally:
            conn.close()

    rate_limits = {}

    while True:
        metrics = {"polled": 0, "triggers": 0, "posted": 0, "dryrun": 0, "rate_limited": 0}
        
        conn = get_db_connection()
        if conn:
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT rowid, topic, content, meta FROM swarm_entries WHERE rowid > ? ORDER BY rowid ASC", (state["last_id"],))
                rows = cursor.fetchall()
                metrics["polled"] = len(rows)
                
                for row_id, topic, content, meta_str in rows:
                    try:
                        meta = json.loads(meta_str) if meta_str else {}
                        if not isinstance(meta, dict):
                            meta = {}
                    except json.JSONDecodeError:
                        meta = {}

                    webhook_url = os.environ.get("SYGNIF_ALERT_WEBHOOK")
                    http_error = False

                    if check_trigger_rules(topic, meta):
                        metrics["triggers"] += 1
                        now = time.time()
                        
                        # Rate limiting per rule (topic)
                        if topic in rate_limits and now - rate_limits[topic] < 300:
                            metrics["rate_limited"] += 1
                        else:
                            # Send alert
                            meta_summary = str(meta)[:100] + "..." if len(str(meta)) > 100 else str(meta)
                            line = f"[{topic}] {content} {meta_summary}"
                            
                            if webhook_url:
                                try:
                                    resp = requests.post(webhook_url, json={"text": line}, timeout=(5, 5))
                                    resp.raise_for_status()
                                    metrics["posted"] += 1
                                    rate_limits[topic] = now
                                except requests.exceptions.Timeout:
                                    print(f"Webhook timeout: {line}", file=sys.stderr)
                                    rate_limits[topic] = now # count as fired so we don't spam timeouts
                                except requests.exceptions.RequestException as e:
                                    print(f"Webhook HTTP error: {e}", file=sys.stderr)
                                    http_error = True
                            else:
                                print(f"[ALERT-DRYRUN] {line}", file=sys.stderr)
                                metrics["dryrun"] += 1
                                rate_limits[topic] = now
                    
                    if http_error:
                        # Break out and don't update last_id for this row, will retry next cycle
                        break

                    state["last_id"] = row_id
                    
                save_state(state)
            except Exception as e:
                print(f"Error reading from database: {e}", file=sys.stderr)
            finally:
                conn.close()

        print(f"[ALERTER HB] polled={metrics['polled']} triggers={metrics['triggers']} posted={metrics['posted']} dryrun={metrics['dryrun']} rate_limited={metrics['rate_limited']}", file=sys.stderr)
        time.sleep(30)

if __name__ == "__main__":
    main()
