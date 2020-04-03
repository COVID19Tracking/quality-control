#
# Flask App for serving check results
#
# To test local:
#     1. run_check_service.py to start a Pyro4 server
#     2. set FLASK_APP=flaskapp.py
#     3. flask run
#     4. browse http://127.0.0.1:5000/


from flask import Flask, render_template
from loguru import logger

from flaskcheck import checks

app = Flask(__name__)
app.register_blueprint(checks)

@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")

if __name__ == "__main__":
    app.run(host='127.0.0.1')
