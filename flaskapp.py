#
# Flask App for serving check results
#

import os
from flask import Flask, request, jsonify, Response
import json
from loguru import logger

from run_check_service import get_proxy

service = get_proxy()

app = Flask(__name__)


@app.route("/checks/working.json", methods=["GET"])
def working_json():
    try:
        result = service.working_json
        return Response(result, mimetype="text/json", status=200)
    except Exception as ex:
        logger.error(f"Exception: {ex}")
        return str(ex), 500


@app.route("/checks/working.html", methods=["GET"])
def working_html():
    try:
        result = service.working_html
        return Response(result, mimetype="text/html", status=200)
    except Exception as ex:
        logger.error(f"Exception: {ex}")
        return str(ex), 500


@app.route("/checks/working.csv", methods=["GET"])
def working_csv():
    try:
        result = service.working_csv
        return Response(result, mimetype="text/csv", status=200)
    except Exception as ex:
        logger.error(f"Exception: {ex}")
        return str(ex), 500

    
@app.route("/", methods=["GET"])
def index():
    return Response(
        b"""
<html>
<body>
    <h3>COVID19 QC Checks v0.01</h3>

    <table>
        <tr>
            <th>DataSet</th><th>HTML</th><th>JSON</th><th>CSV</th>
        </tr>
        <tr>
            <td>WORKING</td>
            <td><a href='/checks/working.html'>/checks/working.html</a></td>
            <td><a href='/checks/working.json'>/checks/working.json</a></td>
            <td><a href='/checks/working.csv'>/checks/working.csv</a></td>
        </tr>
    </table>
</body>
</html>
""",
        mimetype="text/html", status=200)

if __name__ == "__main__":
    app.run(host='0.0.0.0')
