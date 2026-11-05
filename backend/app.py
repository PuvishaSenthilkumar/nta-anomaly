"""Flask application for UNSW-NB15 Intrusion Detection.

Provides REST API endpoints for model prediction and health checks.
Integrates with the existing Network Traffic Analytics project.
"""
from flask import Flask

def create_app():
    """Create and configure the Flask application."""
    app = Flask(__name__)
    app.config.from_pyfile("config.py", silent=True)

    # Import prediction functions and register routes directly
    from backend.predict import load_model, predict_single, predict_batch, api_health

    # Register /api/health route
    @app.route("/api/health", methods=["GET"])
    def health():
        return api_health()

    @app.route("/api/predict", methods=["POST"])
    def api_predict():
        import json
        from flask import request
        data = request.get_json(force=True, silent=True)
        if data is None:
            return jsonify({
                "success": False,
                "error": "No JSON data received"
            }), 400
        result = predict_single(data)
        return jsonify(result), 200 if result.get("success") else 400

    return app

# For direct execution: python backend/app.py
if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=5000, debug=False)