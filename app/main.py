import os
import atexit
from flask import Flask, jsonify, send_from_directory
from flasgger import Swagger

from app.database.connection import close_connection_pool, init_connection_pool
from app.services.worker_threads import init_worker, shutdown_worker
from app.services.logger import get_logger
from app.api.routes import register_routes

logger = get_logger(__name__, level="INFO")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UI_DIR = os.path.normpath(os.path.join(BASE_DIR, "..", "ui"))


def create_flask_app():
    app = Flask(__name__, static_folder=None)

    swagger_config = {
        "headers": [],
        "specs": [
            {
                "endpoint": "apispec",
                "route": "/apispec.json",
                "rule_filter": lambda rule: True,
                "model_filter": lambda tag: True,
            }
        ],
        "static_url_path": "/flasgger_static",
        "swagger_ui": True,
        "specs_route": "/apidocs/",
        "title": "Smart Chat Agent API",
        "uiversion": 3,
    }

    Swagger(app, config=swagger_config)

    @app.route("/health", methods=["GET"])
    def health_check():
        """
        Health Check Endpoint
        ---
        responses:
          200:
            description: Server health status
            schema:
              type: object
              properties:
                status:
                  type: string
                server_active:
                  type: boolean
        """
        return jsonify({
            "status": "healthy",
            "server_active": True
        })

    @app.route("/", methods=["GET"])
    def serve_index():
        return send_from_directory(UI_DIR, "index.html")

    @app.route("/<path:filename>", methods=["GET"])
    def serve_static(filename):
        filepath = os.path.join(UI_DIR, filename)

        if os.path.isfile(filepath):
            return send_from_directory(UI_DIR, filename)

        return jsonify({"error": "not found"}), 404

    register_routes(app)

    return app


app = create_flask_app()

logger.info("Initializing database connection pool...")
init_connection_pool(minconn=1, maxconn=5)

logger.info("Initializing async worker...")
init_worker(max_workers=10)


def cleanup():
    logger.info("Shutting down worker...")
    shutdown_worker()
    close_connection_pool()

atexit.register(cleanup)


if __name__ == "__main__":
    port = int(os.getenv("PORT", 4000))

    logger.warning("Running Flask development server")

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False
    )