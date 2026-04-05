from __future__ import annotations

import logging

from app import create_app


def main() -> None:
    app = create_app("config.yaml")
    app_config = app.config["CONFIG"].get("app", {})
    host = str(app_config.get("host", "127.0.0.1"))
    port = int(app_config.get("port", 5000))
    debug = bool(app_config.get("debug", False))
    logging.getLogger(__name__).info("Starting Flask app on http://%s:%s", host, port)
    app.run(host=host, port=port, debug=debug, use_reloader=False)


if __name__ == "__main__":
    main()
