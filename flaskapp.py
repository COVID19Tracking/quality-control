#
# Flask App for serving check results
# 
# To test local:
#     1. run_check_service.py to start a Pyro4 server
#     2. set FLASK_APP=flaskapp.py
#     3. flask run
#     4. browse http://127.0.0.1:5000/


from flask import Flask, Response
from loguru import logger

from flaskcheck import checks

app = Flask(__name__)
app.register_blueprint(checks)
    
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
