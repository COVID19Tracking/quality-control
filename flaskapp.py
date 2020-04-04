#
# Flask App for serving check results
#
# To test local:
#     1. run_check_service.py to start a Pyro4 server
#     2. set FLASK_APP=flaskapp.py
#     3. flask run
#     4. browse http://127.0.0.1:5000/
#
# To run in production, use gunicorn and wsgi.  see example in _system.
#
import os
from flask import Flask, render_template
from loguru import logger
from datetime import timedelta
from flask import Flask

from flaskcheck import checks, service_load_dates

# register dynamically
#@route("/", methods=["GET"])
def index():
    site_at, service_at, server_now = service_load_dates()

    def format_delta(td: timedelta) -> str:
        s = int(td.total_seconds())
        if s < 60: return f"{s} sec" + ("s" if s == 1 else "")
        m = s // 60
        if m < 60: return f"{m} mins" + ("s" if m == 1 else "")
        h = m // 60
        if h < 24: return f"{h} hours" + ("s" if h == 1 else "")
        d = h // 24
        return f"{d} days" + ("s" if d == 1 else "")

    site_delta = format_delta(server_now - site_at)
    service_delta = format_delta(server_now - service_at)

    server_now = server_now.isoformat()
    return render_template("index.html", 
        server_now=server_now,
        site_delta=site_delta, 
        service_delta=service_delta)

def create_app() -> Flask:
    app = Flask(__name__)
    app.register_blueprint(checks)

    app.add_url_rule("/", 'index', index, methods=["GET"])
    return app

if __name__ == "__main__":
    app = create_app()
    app.run(host='127.0.0.1')
