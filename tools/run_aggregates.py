import os
import sys

# Ensure repository root is on sys.path when executed as a script from tools/
REPO_ROOT = os.path.dirname(os.path.dirname(__file__))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from fetch_repos.db import Database
from fetch_repos.aggregator import compute_aggregates


def main():
    db_path = sys.argv[1] if len(sys.argv) > 1 else os.getenv("DB_PATH", "data.db")
    namespace = None
    if len(sys.argv) > 2:
        namespace = sys.argv[2]
    else:
        namespace = os.getenv("DB_NAMESPACE")
    db = Database(db_path, namespace=namespace)
    compute_aggregates(db)

    cur = db.conn.cursor()
    cur.execute("SELECT metric, key, value, extra_json FROM aggregates ORDER BY metric, key;")
    for metric, key, value, extra_json in cur.fetchall():
        print(metric, key, value, extra_json or "")


if __name__ == "__main__":
    main()
