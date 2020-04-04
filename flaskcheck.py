#
# A Flask Blueprint to support embedding checks as a library
#

import os
from flask import Blueprint, request, jsonify, Response, render_template
import json
from typing import Tuple
from datetime import datetime
from loguru import logger

from run_quality_service import get_proxy
import app.util.udatetime as udatetime

checks = Blueprint("checks", __name__, url_prefix='/checks')

load_date = udatetime.now_as_eastern()

def service_load_dates() -> Tuple[datetime, datetime, datetime]:
    " returns flask app start time, Pyro4 service start time, and current time (all ET)"
    try:
        service = get_proxy()
        service_date = datetime.fromisoformat(service.load_date)
        return load_date, service_date, udatetime.now_as_eastern() 
    except Exception as ex:
        logger.exception(ex)
        return load_date, None, udatetime.now_as_eastern()


@checks.route("/working.json", methods=["GET"])
def working_json():
    try:
        service = get_proxy()
        result = service.working_json
        return Response(result, mimetype="text/json", status=200)
    except Exception as ex:
        logger.exception(f"Exception: {ex}")
        return str(ex), 500


@checks.route("/working.html", methods=["GET"])
def working_html():
    try:
        service = get_proxy()
        result = service.working_html
        return render_template("check_results.html", result=result)
    except Exception as ex:
        logger.exception(f"Exception: {ex}")
        return str(ex), 500


@checks.route("/working.csv", methods=["GET"])
def working_csv():
    try:
        service = get_proxy()
        result = service.working_csv
        return Response(result, mimetype="text/csv", status=200)
    except Exception as ex:
        logger.exception(f"Exception: {ex}")
        return str(ex), 500

@checks.route("/current.json", methods=["GET"])
def current_json():
    try:
        service = get_proxy()
        result = service.current_json
        return Response(result, mimetype="text/json", status=200)
    except Exception as ex:
        logger.exception(f"Exception: {ex}")
        return str(ex), 500


@checks.route("/current.html", methods=["GET"])
def current_html():
    try:
        service = get_proxy()
        result = service.current_html
        return render_template("check_results.html", result=result)
    except Exception as ex:
        logger.exception(f"Exception: {ex}")
        return str(ex), 500


@checks.route("/current.csv", methods=["GET"])
def current_csv():
    try:
        service = get_proxy()
        result = service.current_csv
        return Response(result, mimetype="text/csv", status=200)
    except Exception as ex:
        logger.exception(f"Exception: {ex}")
        return str(ex), 500


@checks.route("/history.json", methods=["GET"])
def history_json():
    try:
        service = get_proxy()
        result = service.history_json
        return Response(result, mimetype="text/json", status=200)
    except Exception as ex:
        logger.exception(f"Exception: {ex}")
        return str(ex), 500


@checks.route("/history.html", methods=["GET"])
def history_html():
    try:
        service = get_proxy()
        result = service.history_html
        return render_template("check_results.html", result=result)
    except Exception as ex:
        logger.exception(f"Exception: {ex}")
        return str(ex), 500


@checks.route("/history.csv", methods=["GET"])
def history_csv():
    try:
        service = get_proxy()
        result = service.history_csv
        return Response(result, mimetype="text/csv", status=200)
    except Exception as ex:
        logger.exception(f"Exception: {ex}")
        return str(ex), 500
