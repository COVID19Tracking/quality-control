#
# A Flask Blueprint to support embedding checks as a library
#

import os
from flask import Blueprint, request, jsonify, Response
import json
from loguru import logger

from run_quality_service import get_proxy

checks = Blueprint("checks", __name__)

@checks.route("/checks/working.json", methods=["GET"])
def working_json():
    try:
        service = get_proxy()
        result = service.working_json
        return Response(result, mimetype="text/json", status=200)
    except Exception as ex:
        logger.error(f"Exception: {ex}")
        return str(ex), 500


@checks.route("/checks/working.html", methods=["GET"])
def working_html():
    try:
        service = get_proxy()
        result = service.working_html
        return Response(result, mimetype="text/html", status=200)
    except Exception as ex:
        logger.error(f"Exception: {ex}")
        return str(ex), 500


@checks.route("/checks/working.csv", methods=["GET"])
def working_csv():
    try:
        service = get_proxy()
        result = service.working_csv
        return Response(result, mimetype="text/csv", status=200)
    except Exception as ex:
        logger.error(f"Exception: {ex}")
        return str(ex), 500


