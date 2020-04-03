#
# A Flask Blueprint to support embedding checks as a library
#

import os
from flask import Blueprint, request, jsonify, Response, render_template
import json
from loguru import logger

from run_quality_service import get_proxy

checks = Blueprint("checks", __name__, url_prefix='/checks')

@checks.route("/working.json", methods=["GET"])
def working_json():
    try:
        service = get_proxy()
        result = service.working_json
        return Response(result, mimetype="text/json", status=200)
    except Exception as ex:
        logger.error(f"Exception: {ex}")
        return str(ex), 500


@checks.route("/working.html", methods=["GET"])
def working_html():
    try:
        service = get_proxy()
        result = service.working_html
        return render_template("check_results.html", result=result)
    except Exception as ex:
        logger.error(f"Exception: {ex}")
        return str(ex), 500


@checks.route("/working.csv", methods=["GET"])
def working_csv():
    try:
        service = get_proxy()
        result = service.working_csv
        return Response(result, mimetype="text/csv", status=200)
    except Exception as ex:
        logger.error(f"Exception: {ex}")
        return str(ex), 500

@checks.route("/current.json", methods=["GET"])
def current_json():
    try:
        service = get_proxy()
        result = service.current_json
        return Response(result, mimetype="text/json", status=200)
    except Exception as ex:
        logger.error(f"Exception: {ex}")
        return str(ex), 500


@checks.route("/current.html", methods=["GET"])
def current_html():
    try:
        service = get_proxy()
        result = service.current_html
        return render_template("check_results.html", result=result)
    except Exception as ex:
        logger.error(f"Exception: {ex}")
        return str(ex), 500


@checks.route("/current.csv", methods=["GET"])
def current_csv():
    try:
        service = get_proxy()
        result = service.current_csv
        return Response(result, mimetype="text/csv", status=200)
    except Exception as ex:
        logger.error(f"Exception: {ex}")
        return str(ex), 500


@checks.route("/history.json", methods=["GET"])
def history_json():
    try:
        service = get_proxy()
        result = service.history_json
        return Response(result, mimetype="text/json", status=200)
    except Exception as ex:
        logger.error(f"Exception: {ex}")
        return str(ex), 500


@checks.route("/history.html", methods=["GET"])
def history_html():
    try:
        service = get_proxy()
        result = service.history_html
        return render_template("check_results.html", result=result)
    except Exception as ex:
        logger.error(f"Exception: {ex}")
        return str(ex), 500


@checks.route("/history.csv", methods=["GET"])
def history_csv():
    try:
        service = get_proxy()
        result = service.history_csv
        return Response(result, mimetype="text/csv", status=200)
    except Exception as ex:
        logger.error(f"Exception: {ex}")
        return str(ex), 500
