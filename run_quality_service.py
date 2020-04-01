#
#  Pyro4 Service -- Cache and Run QC Checks 
#
#  Hold the cache results on a singleton RPC server
#
import util
import udatetime
import Pyro4
from loguru import logger

from check_dataset import check_working  

from result_log import ResultLog
from data_source import DataSource
from qc_config import QCConfig


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
    def reset(self):
        self._working = None
        self._current = None
        self._history = None

        config = util.read_config_file("quality-control")
        self.config = QCConfig(
            results_dir=config["CHECKS"]["results_dir"], 
            enable_experimental=config["CHECKS"]["enable_experimental"] == "True",
            save_results=config["CHECKS"]["save_results"] == "True",
            images_dir=config["MODEL"]["images_dir"], 
            plot_models=config["MODEL"]["plot_models"] == "True",
        )

        self.ds = DataSource()

    # --- working data
    @property 
    def working(self) -> ResultLog:
        if is_out_of_date(self._working, 60):
            self._working = check_working(self.ds, self.config)  
        return self._working

    @Pyro4.expose
    @property
    def working_csv(self) -> str:
        return self.working.to_csv()
    @Pyro4.expose
    @property
    def working_json(self) -> str:
        return self.working.to_json()
    @Pyro4.expose
    @property
    def working_html(self) -> str:
        return self.working.to_html()

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

