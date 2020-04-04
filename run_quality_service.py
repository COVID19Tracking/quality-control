#
#  Pyro4 Service -- Cache and Run QC Checks
#
#  Hold the cache results on a singleton RPC server
#
import Pyro4
from loguru import logger
from datetime import datetime

from app.check_dataset import check_working, check_current, check_history

from app.log.result_log import ResultLog
from app.data.data_source import DataSource
from app.qc_config import QCConfig
import app.util.util as util
import app.util.udatetime as udatetime

load_date = udatetime.now_as_eastern()

def is_out_of_date(log: ResultLog, cache_seconds: int) -> bool:
    " check if result is out-of-date"
    if log == None: return True
    dt = udatetime.now_as_eastern()
    delta = dt - log.loaded_at
    return delta.total_seconds() > cache_seconds


class CheckServer:
    "cache the check results for 60 seconds"

    def __init__(self):
        self.reset()

    @Pyro4.expose
    @property
    def load_date(self) -> datetime:
        return load_date

    @Pyro4.expose
    def reset(self):
        self._working = None
        self._current = None
        self._history = None

        config = util.read_config_file("quality-control")
        self.config = QCConfig(
            results_dir=config["CHECKS"]["results_dir"],
            enable_experimental=config["CHECKS"]["enable_experimental"] == "True",
            enable_debug=config["CHECKS"]["enable_debug"] == "True",
            save_results=config["CHECKS"]["save_results"] == "True",
            images_dir=config["MODEL"]["images_dir"],
            plot_models=config["MODEL"]["plot_models"] == "True",
        )

        self.ds = DataSource()

    # --- working data
    @property
    def working(self) -> ResultLog:
        if is_out_of_date(self._working, 60):
            self.ds = DataSource()
            self._working = check_working(self.ds, self.config)
        return self._working

    @Pyro4.expose
    @property
    def working_csv(self) -> str:
        w =  self.working
        if w is None:
            return self.ds.log.to_csv()
        else:
            return w.to_csv()

    @Pyro4.expose
    @property
    def working_json(self) -> str:
        w =  self.working
        if w is None:
            return self.ds.log.to_json()
        else:
            return w.to_json()

    @Pyro4.expose
    @property
    def working_html(self) -> str:
        w =  self.working
        if w is None:
            return self.ds.log.to_html()
        else:
            return w.to_html()

# -----------------------------------
# --- current data
    @property
    def current(self) -> ResultLog:
        if is_out_of_date(self._current, 60):
            self.ds = DataSource()
            self._current = check_current(self.ds, self.config)
        return self._current

    @Pyro4.expose
    @property
    def current_csv(self) -> str:
        c =  self.current
        if c is None:
            return self.ds.log.to_csv()
        else:
            return c.to_csv()

    @Pyro4.expose
    @property
    def current_json(self) -> str:
        c =  self.current
        if c is None:
            return self.ds.log.to_json()
        else:
            return c.to_json()

    @Pyro4.expose
    @property
    def current_html(self) -> str:
        c =  self.current
        if c is None:
            return self.ds.log.to_html()
        else:
            return c.to_html()

# -----------------------------------
# --- history data
    @property
    def history(self) -> ResultLog:
        if is_out_of_date(self._history, 60):
            self.ds = DataSource()
            self._history = check_history(self.ds)
        return self._history

    @Pyro4.expose
    @property
    def history_csv(self) -> str:
        h =  self.history
        if h is None:
            return self.ds.log.to_csv()
        else:
            return h.to_csv()

    @Pyro4.expose
    @property
    def history_json(self) -> str:
        h =  self.history
        if h is None:
            return self.ds.log.to_json()
        else:
            return h.to_json()

    @Pyro4.expose
    @property
    def history_html(self) -> str:
        h =  self.history
        if h is None:
            return self.ds.log.to_html()
        else:
            return h.to_html()

# -----------------------------------

HOST = "localhost"
PORT = 8201
VERSION = "1.0"
KEY= "covid-qc"

def start_server():
    daemon = Pyro4.Daemon(host=HOST, port=PORT)
    daemon._pyroHmacKey = KEY
    uri = daemon.register(CheckServer, objectId="checkServer")

    logger.info(f"CheckServer Ready, objectId=checkServer, host={HOST}, port={PORT}, version={VERSION}")
    logger.info(f"  uri={uri}")
    daemon.requestLoop()

def get_proxy() -> CheckServer:
    url = f"PYRO:checkServer@{HOST}:{PORT}"

    logger.info(f"connect to {url}")
    server = Pyro4.Proxy(url)
    server._pyroHmacKey = KEY
    logger.info("ready")

    return server

if __name__ == '__main__':
    start_server()

