import argparse
from backend.api import app
from backend.auto_refresh import start as start_auto_refresh


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=5001)
    parser.add_argument("--no-refresh", action="store_true",
                        help="serve only; skip the background data refresh")
    args = parser.parse_args()
    if not args.no_refresh:
        start_auto_refresh()
    print(f"Dashboard: http://localhost:{args.port}")
    app.run(host="0.0.0.0", port=args.port, debug=False)


if __name__ == "__main__":
    main()
