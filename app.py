# ==============================================================================
# File: app.py (Python Backend)
# ==============================================================================
# This is the core Python script that runs the data analysis as a web service.

import pandas as pd
import warnings
import logging
from flask import Flask, request, jsonify, send_file
import io
import os
import sys
import subprocess

# Suppress warnings for cleaner output
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
warnings.filterwarnings("ignore", "invalid value encountered in subtract", RuntimeWarning)

# We need to ensure openpyxl and xlsxwriter are available in the deployment environment
try:
    import openpyxl
except ImportError:
    print("openpyxl not found, attempting to install...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "openpyxl"])
    import openpyxl

try:
    import xlsxwriter
except ImportError:
    print("xlsxwriter not found, attempting to install...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "xlsxwriter"])
    import xlsxwriter

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50 MB upload limit

# --- FIX: New import from the external analysis file ---
from analyze_data_core import analyze_data_core
# --- END FIX ---

# ----------------------------------------
# Health Check
# ----------------------------------------
@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "ok"}), 200

# ----------------------------------------
# HTML Landing Page
# ----------------------------------------
@app.route('/', methods=['GET'])
def index():
    return send_file('index.html')

# ----------------------------------------
# Main Analyze Endpoint
# ----------------------------------------
@app.route('/analyze', methods=['POST'])
def analyze():
    if 'raw_data_file' not in request.files:
        return jsonify({"error": "No raw data file provided"}), 400

    raw_data_file = request.files['raw_data_file']
    fp_file = request.files.get('fp_file') # False positive file is optional

    try:
        # Read the Excel file into a BytesIO stream
        raw_data_stream = io.BytesIO(raw_data_file.read())

        # Read the false positive file into a BytesIO stream
        fp_stream = None
        if fp_file and fp_file.filename != '':
            fp_stream = io.BytesIO(fp_file.read())

        # --- FIX: Call the external core analysis function with streams ---
        output_stream = analyze_data_core(raw_data_stream, fp_stream)
        # --- END FIX ---

    except Exception as e:
        logging.error(f"Analysis error: {e}")
        return jsonify({"error": f"Analysis failed: {e}"}), 500

    return send_file(output_stream, as_attachment=True, download_name='cleaned_data.xlsx')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
