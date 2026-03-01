from api.endpoints.chat import chat_bp
from api.endpoints.documents import documents_bp


def register_routes(app):
    """Register all API blueprints on the Flask app."""
    app.register_blueprint(chat_bp, url_prefix="/api")
    app.register_blueprint(documents_bp, url_prefix="/api")