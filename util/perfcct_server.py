#!/usr/bin/env python3
"""Serve a PerfCCT lifetime.db to perfcct_viewer.html via on-demand SQL.

Instead of loading the whole database into the browser (sql.js, which caps
out around a couple hundred MB), the viewer sends each SELECT to this small
backend, which runs it against the on-disk SQLite file and returns just the
matching rows as JSON. Memory stays bounded no matter how big the db is, so
full-simpoint traces (multi-GB) can be explored interactively.

Usage:
    python3 util/perfcct_server.py --db path/to/lifetime.db \\
        [--port 8000] [--viewer util/perfcct_viewer.html]

Then open http://localhost:<port>/ in a browser and start exploring.

The main db is opened read-only; the viewer's TEMP tables (io_ids) live in
SQLite's separate temp database, so they still work. A single connection is
shared behind a lock so those TEMP tables persist across requests.
"""

import argparse
import json
import os
import sqlite3
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

CONN = None
LOCK = threading.Lock()
DB_PATH = ""
VIEWER_PATH = ""

# Indexes the viewer's hot-path queries need. Without them, every panel /
# pagination query full-scans the whole table, which is unusable on multi-GB
# dbs. Built once (persisted in the file) via --index.
INDEXES = [
    ("LifeTimeCommitTrace", "idx_ltct_fetch", "AtFetch"),
    ("LifeTimeCommitTrace", "idx_ltct_commit", "AtCommit"),
    ("LifeTimeCommitTrace", "idx_ltct_stall", "StallCycles"),
    ("SquashedLifeTimeTrace", "idx_sltct_fetch", "AtFetch"),
    ("SquashedLifeTimeTrace", "idx_sltct_commit", "AtCommit"),
]


def ensure_indexes(path):
    """Create the viewer's indexes if missing (one-time, mutates the db)."""
    conn = sqlite3.connect(path)  # read-write
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    have = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index'")}
    for tbl, idx, col in INDEXES:
        if tbl in tables and idx not in have:
            print(f"  building index {idx} on {tbl}({col}) ...", flush=True)
            conn.execute(f"CREATE INDEX {idx} ON {tbl}({col})")
    # Without stats the planner can't tell that guards like `AtFetch>0` match
    # ~every row, so it wastes an index there and TEMP-B-TREE-sorts the ORDER BY
    # (a ~50s full sort on huge dbs). A sampled ANALYZE is cheap and lets it pick
    # the ordering index for sorts instead.
    print("  running sampled ANALYZE ...", flush=True)
    conn.execute("PRAGMA analysis_limit=1000")
    conn.execute("ANALYZE")
    conn.commit()
    conn.close()
    print("  indexes ready", flush=True)


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, body, ctype="application/json"):
        data = body if isinstance(body, (bytes, bytearray)) else body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        try:
            self.wfile.write(data)
        except BrokenPipeError:
            pass

    def do_GET(self):
        if self.path in ("/", "/index.html"):
            try:
                with open(VIEWER_PATH, "rb") as f:
                    html = f.read()
            except OSError as e:
                self._send(500, json.dumps({"error": str(e)}))
                return
            self._send(200, html, "text/html; charset=utf-8")
        elif self.path == "/name":
            self._send(200, os.path.basename(DB_PATH),
                       "text/plain; charset=utf-8")
        else:
            self._send(404, json.dumps({"error": "not found"}))

    def do_POST(self):
        if self.path != "/q":
            self._send(404, json.dumps({"error": "not found"}))
            return
        n = int(self.headers.get("Content-Length", 0))
        sql = self.rfile.read(n).decode("utf-8")
        try:
            with LOCK:
                cur = CONN.execute(sql)
                rows = cur.fetchall()
            self._send(200, json.dumps({"values": [list(r) for r in rows]}))
        except Exception as e:  # surface SQL errors to the viewer
            self._send(400, json.dumps({"error": str(e)}))

    def log_message(self, *args):
        pass  # keep the console quiet


def main():
    global CONN, DB_PATH, VIEWER_PATH
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", required=True, help="path to lifetime.db")
    ap.add_argument("--port", type=int, default=8000, help="listen port")
    ap.add_argument("--viewer",
                    default=os.path.join(here, "perfcct_viewer.html"),
                    help="path to perfcct_viewer.html")
    ap.add_argument("--index", action="store_true",
                    help="build the AtFetch/AtCommit indexes the viewer needs "
                    "before serving (one-time per db; required for responsive "
                    "browsing of large dbs)")
    args = ap.parse_args()

    DB_PATH = os.path.abspath(args.db)
    VIEWER_PATH = os.path.abspath(args.viewer)
    if not os.path.exists(DB_PATH):
        ap.error(f"db not found: {DB_PATH}")
    if not os.path.exists(VIEWER_PATH):
        ap.error(f"viewer not found: {VIEWER_PATH}")

    if args.index:
        print(f"ensuring indexes on {DB_PATH} (one-time) ...")
        ensure_indexes(DB_PATH)

    # read-only main db; TEMP tables still allowed (separate temp db)
    CONN = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True,
                           check_same_thread=False)
    CONN.execute("PRAGMA temp_store=MEMORY")

    srv = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(f"PerfCCT viewer serving {DB_PATH}")
    print(f"  open  http://localhost:{args.port}/")
    print("  Ctrl-C to stop")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nstopping")
    finally:
        srv.server_close()
        CONN.close()


if __name__ == "__main__":
    main()
