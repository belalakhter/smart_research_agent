import os
from flask import Flask, jsonify
from database.connection import init_connection_pool
from services.worker_threads import init_worker, shutdown_worker
from services.logger import get_logger
from api.routes import register_routes

logger = get_logger(__name__, level="INFO")


def create_flask_app():
    app = Flask(__name__)

    @app.route("/health", methods=["GET"])
    def health_check():
        return jsonify({"status": "healthy", "server_active": True})

    register_routes(app)
    return app


if __name__ == "__main__":
    logger.info("Initializing async worker...")
    init_worker(max_workers=10)

    init_connection_pool(minconn=1, maxconn=5)
    logger.info("Database initialized...")

    app = create_flask_app()
    port = int(os.getenv("PORT", 3000))
    logger.info(f"Starting Flask server on port {port}...")

    try:
        app.run(host="0.0.0.0", port=port)
    finally:
        logger.info("Shutting down worker...")
        shutdown_worker()