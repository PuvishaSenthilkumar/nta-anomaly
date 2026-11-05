"""Prediction service for UNSW-NB15 Random Forest model.

Loads the pre-trained scikit-learn Pipeline and provides prediction functions
and Flask API endpoints for network traffic classification.
"""
import joblib
import numpy as np
import pandas as pd
from flask import Flask, request, jsonify

# Global model variable
_model = None
_feature_names = None

# Expected feature order - must match training time
# These are the 43 features used during training (id, label, attack_cat excluded)
EXPECTED_FEATURES = [
    "dur", "proto", "service", "state", "spkts", "dpkts", "sbytes", "dbytes", "rate",
    "sttl", "dttl", "sload", "dload", "sloss", "dloss", "sinpkt", "dinpkt", "sjit",
    "djit", "swin", "stcpb", "dtcpb", "dwin", "tcprtt", "synack", "ackdat", "smean",
    "dmean", "trans_depth", "response_body_len", "ct_srv_src", "ct_state_ttl", "ct_dst_ltm",
    "ct_src_dport_ltm", "ct_dst_sport_ltm", "ct_dst_src_ltm", "is_ftp_login", "ct_ftp_cmd",
    "ct_flw_http_mthd", "ct_src_ltm", "ct_srv_dst", "is_sm_ips_ports"
]


def load_model():
    """Load the trained model pipeline.

    Returns True if successful, False otherwise.
    """
    global _model, _feature_names

    import os
    # Compute from project root: .. from backend/predict.py gives project root
    # Project root is two dirs up from backend/predict.py
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    # Build path: project_root + /backend/models/UNSW_NB15_Model.pkl
    full_path = os.path.join(project_root, "backend", "models", "UNSW_NB15_Model.pkl")

    if not os.path.exists(full_path):
        print(f"Model file not found: {full_path}")
        return False

    try:
        _model = joblib.load(full_path)
        # Extract feature names from the pipeline
        if hasattr(_model, 'named_steps'):
            # Pipeline: get feature names from the last step
            step_keys = list(_model.named_steps.keys())
            last_step_name = step_keys[-1] if step_keys else None
            if last_step_name and hasattr(_model.named_steps[last_step_name], 'feature_names_in_'):
                _feature_names = list(_model.named_steps[last_step_name].feature_names_in_)
            elif last_step_name and hasattr(_model.named_steps[last_step_name], 'ohe') and hasattr(_model.named_steps[last_step_name].ohe, 'feature_names_in_'):
                _feature_names = list(_model.named_steps[last_step_name].ohe.feature_names_in_)
            else:
                _feature_names = EXPECTED_FEATURES
        else:
            _feature_names = EXPECTED_FEATURES

        print(f"Model loaded successfully from {full_path}")
        print(f"Model type: {type(_model).__name__}")
        print(f"Number of features expected: {len(_feature_names)}")
        return True

    except Exception as e:
        print(f"Error loading model: {e}")
        _model = None
        _feature_names = None
        return False


def predict_single(features_dict):
    """Predict classification for a single traffic flow.

    Args:
        features_dict: Dictionary with feature names as keys and values.

    Returns:
        dict with prediction, classification, confidence, and message.
    """
    if _model is None:
        return {
            "success": False,
            "error": "Model not loaded"
        }

    try:
        # Convert to DataFrame with correct feature order
        features_list = []
        missing = []
        for fname in _feature_names:
            if fname in features_dict:
                try:
                    features_list.append(float(features_dict[fname]))
                except (ValueError, TypeError):
                    features_list.append(0.0)
            else:
                missing.append(fname)
                features_list.append(0.0)

        if missing:
            print(f"Warning: Missing features: {missing}")

        # Create DataFrame with exactly the expected feature names and order
        df = pd.DataFrame([features_list], columns=_feature_names)

        # The pipeline handles preprocessing internally
        prediction = _model.predict(df)[0]
        confidence = float(_model.predict_proba(df)[0][prediction])
        class_labels = {0: "Normal", 1: "Attack"}
        classification = class_labels.get(prediction, str(prediction))

        return {
            "success": True,
            "prediction": int(prediction),
            "classification": classification,
            "confidence": round(confidence, 4),
            "message": f"{classification} network traffic detected"
        }

    except Exception as e:
        print(f"Prediction error: {e}")
        return {
            "success": False,
            "error": str(e)
        }


def predict_batch(csv_filepath):
    """Predict classifications for multiple records from a CSV file.

    Args:
        csv_filepath: Path to CSV file containing UNSW-NB15 features.

    Returns:
        dict with results, summary, and failed records.
    """
    if _model is None:
        return {
            "success": False,
            "error": "Model not loaded"
        }

    try:
        df = pd.read_csv(csv_filepath)

        # Filter out columns not expected by the model
        # Remove id, label, attack_cat if present
        cols_to_drop = [c for c in ["id", "label", "attack_cat"] if c in df.columns]
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)

        # Ensure we only keep features the model expects
        input_features = {}
        for col in df.columns:
            if col in _feature_names:
                input_features[col] = df[col].values

        # Build records for prediction
        results = []
        for i in range(len(df)):
            record_features = {}
            for fname in _feature_names:
                if fname in input_features:
                    val = input_features[fname][i]
                    try:
                        record_features[fname] = float(val)
                    except (ValueError, TypeError):
                        record_features[fname] = 0.0
                else:
                    record_features[fname] = 0.0

            pred = predict_single(record_features)
            if pred["success"]:
                pred["record_id"] = i
                # Include original features that are in the model but not selectively passed
                results.append(pred)
            else:
                results.append({
                    "record_id": i,
                    "success": False,
                    "error": pred.get("error", "Prediction failed")
                })

        # Compute summary
        total = len([r for r in results if r.get("success")])
        attacks = len([r for r in results if r.get("success") and r.get("prediction") == 1])
        normals = total - attacks
        attack_pct = round(attacks / total * 100, 2) if total > 0 else 0

        summary = {
            "total_records": total + len([r for r in results if not r.get("success")]),
            "normal_count": normals,
            "attack_count": attacks,
            "attack_percentage": attack_pct
        }

        return {
            "success": True,
            "results": results,
            "summary": summary
        }

    except Exception as e:
        print(f"Batch prediction error: {e}")
        return {
            "success": False,
            "error": str(e)
        }


# Flask app setup
app = Flask(__name__)

@app.route("/api/predict", methods=["POST"])
def api_predict():
    """Predict classification for a single network traffic flow.

    Expected JSON input:
    {
        "dur": 1.5,
        "proto": "tcp",
        "service": "http",
        "state": "FIN",
        ...
        (all 43 expected features)
    }
    """
    try:
        data = request.get_json(force=True, silent=True)
        if data is None:
            return jsonify({
                "success": False,
                "error": "No JSON data received"
            }), 400

        result = predict_single(data)
        return jsonify(result), 200 if result.get("success") else 400

    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"Request processing error: {str(e)}"
        }), 500


@app.route("/api/health", methods=["GET"])
def api_health():
    """Verify that the model is loaded and the prediction service is operational."""
    return jsonify({
        "status": "healthy" if _model is not None else "unhealthy",
        "model_loaded": _model is not None,
        "features_expected": len(_feature_names) if _feature_names else 0,
        "model_path": "backend/models/UNSW_NB15_Model.pkl"
    }), 200


# Module-level model loading when this file is imported
# The model is loaded on import so it's available when the Flask app starts
_model_loaded = load_model()

# Note: In production, you would run this with:
#   python -m backend.predict  or
#   flask --app backend.predict.run serve
# The load_model() call at module level ensures the model is ready when the app starts.

if __name__ == "__main__":
    # If model not loaded at import time, try loading now
    if _model is None:
        load_model()
    app.run(host="0.0.0.0", port=5000, debug=False)