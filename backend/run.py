import argparse
from backend.api import app
from backend.auto_refresh import start as start_auto_refresh
from backend.storage import DB_PATH

# Below this size the DB is an empty/stub file (a populated cmm.db is >100 MB).
_MIN_DB_BYTES = 1_000_000


def _db_is_populated() -> bool:
    return DB_PATH.exists() and DB_PATH.stat().st_size >= _MIN_DB_BYTES


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=5001)
    parser.add_argument("--no-refresh", action="store_true",
                        help="serve only; skip the background data refresh")
    args = parser.parse_args()

    if not _db_is_populated():
        if args.no_refresh:
            print(f"WARNING: {DB_PATH} is missing or empty and --no-refresh is set — "
                  "the dashboard will serve empty panels.\n"
                  "Rerun without --no-refresh to build the database automatically, or "
                  "run `python -m backend.bootstrap_db` to build it in the foreground.")
        else:
            print(f"{DB_PATH} is missing or empty — building it from public sources "
                  "in the background (no API keys needed).\n"
                  "The dashboard is usable immediately; panels fill in as fetches "
                  "complete (news first, the full batch takes hours).\n"
                  "Progress: http://localhost:%d/api/refresh/status or "
                  "data/logs/auto_refresh.log" % args.port)

    if not args.no_refresh:
        start_auto_refresh()
    print(f"Dashboard: http://localhost:{args.port}")
    from waitress import serve
    serve(app, host="0.0.0.0", port=args.port, threads=8)


if __name__ == "__main__":
    main()
