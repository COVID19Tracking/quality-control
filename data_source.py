#
# Manages all the data needed for checks:
#
#    1. The DEV worksheet in Google sheets
#    2. The historical data pulled from the API
#    3. The current data pulled from the API (redundant w/the historical data)
#
# This module is responsible for type conversion and renaming the fields for consistency.
#

from typing import List, Dict
from loguru import logger
import pandas as pd
import numpy as np
import re
import udatetime

from worksheet_wrapper import WorksheetWrapper

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
KEY_PATH = "credentials-scanner.json"

class DataSource:

    def __init__(self):
        self._working: pd.DataFrame = None
        self._history: pd.DataFrame = None
        self._current: pd.DataFrame = None


    @property
    def working(self) -> pd.DataFrame:
        " the working dataset"
        if self._working is None:
            self._working = self.load_working()
        return self._working

    @property
    def history(self) -> pd.DataFrame:
        " the daily history dataset"
        if self._history is None:
            self._history = self.load_history()
        return self._history

    @property
    def current(self) -> pd.DataFrame:
        " the today's dataset"
        if self._current is None:
            self._current = self.load_current()
        return self._current


    def load_working(self) -> pd.DataFrame:
        """Load the working (unpublished) data from google sheets"""

        # make dev columns match api columns so quality
        # checks run with both inputs
        column_map = {
            'TESTING & OUTCOMES State':'state',
            'TESTING & OUTCOMES Positive':'positive',
            'TESTING & OUTCOMES Negative':'negative',
            'TESTING & OUTCOMES Pending':'pending',
            'Hospitalized Current':'hospitalized',
            'Hospitalized Cumulative':'hospitalizedCumulative',
            'ICU Current':'icu',
            'ICU Cumulative':'icuCumulative',
            'Ventilator Current':'ventilator',
            'Ventilator Cumulative':'ventilatorCumulative',
            'Ventilator Recovered':'recovered', # api misreads from sheet
            'Ventilator Deaths':'death', # api misreads from sheet
            'CALCULATED Total':'total',
            'CALCULATED Last Update (ET)':'lastUpdateEt',
            'CHECKER METADATA Last Check (ET)':'lastCheckEt',
            'Checker':'checker',
            'Double Checker':'doubleChecker'
        }

        gs = WorksheetWrapper()
        dev_id = gs.get_sheet_id_by_name("dev")
        df = gs.read_as_frame(dev_id, "Worksheet!G2:W60", header_rows=2)

        # check names and rename/suppress columns
        names = []
        for n in df.columns.values:
            n2 = column_map.get(n)
            if n2 == None:
                logger.warning(f"  Unexpected column: {n} in google sheet")
            if n2 == '': 
                del df[n]
            else:
                names.append(n2)

        df.columns = names

        idx = df.columns.get_loc("lastUpdateEt")

        for c in df.columns[1:idx]:
            df[c] = df[c].str.strip().replace("", "0").replace(re.compile(","), "")
            df[c] = df[c].astype(np.int)

        def convert_date(s: pd.Series) -> pd.Series:
            s = s.replace('', "01/01 00:00") # whole string match
            s = s.str.replace(' ', "/2020 ") # partial string match
            s = pd.to_datetime(s, format="%m/%d/%Y %H:%M") \
                .apply(udatetime.pandas_timestamp_as_eastern)
            return s
        
        # remove current time from first riate
        current_time = df.loc[0, "lastCheckEt"].replace("CURRENT NAME: ", "")
        df.loc[0, "lastCheckEt"] = ""

        df["lastUpdateEt"] = convert_date(df["lastUpdateEt"])
        df["lastCheckEt"] = convert_date(df["lastCheckEt"])

        df = df[ df.state != ""]
        return df

    def load_current(self) -> pd.DataFrame:
        """ load the current values from the API """

        df = pd.read_csv("https://covidtracking.com/api/states.csv").fillna(0)
        df["lastUpdateEt"] = pd.to_datetime(df["lastUpdateEt"].str.replace(" ", "/2020 "), format="%m/%d/%Y %H:%M") \
            .apply(udatetime.pandas_timestamp_as_eastern)
        df["checkTimeEt"] = pd.to_datetime(df["checkTimeEt"].str.replace(" ", "/2020 "), format="%m/%d/%Y %H:%M") \
            .apply(udatetime.pandas_timestamp_as_eastern)
        df["dateModified"] = pd.to_datetime(df["dateModified"])
        df["dateChecked"] = pd.to_datetime(df["dateChecked"])

        df.fillna(0.0, inplace=True)

        # counts
        for c in ["positive", "negative", "pending", "hospitalized", "death", "total", "totalTestResults"]:
            df[c] = df[c].astype(np.int)

        # 0 or 1.  score = sum of others so it is 0-4
        for c in ["positiveScore", "negativeScore", "negativeRegularScore", "commercialScore", "score"]:
            df[c] = df[c].astype(np.int)
        return df

    def load_history(self) -> pd.DataFrame:
        """ load daily values over time from the API """

        df = pd.read_csv("https://covidtracking.com/api/states/daily.csv")

        df.fillna(0.0, inplace=True)

        # counts
        for c in ["positive", "negative", "pending", "hospitalized", "death", "total", "totalTestResults"]:
            df[c] = df[c].astype(np.int)
        for c in ["positiveIncrease", "negativeIncrease", "hospitalizedIncrease", "deathIncrease", "totalTestResultsIncrease"]:
            df[c] = df[c].astype(np.int)

        df["dateChecked"] = pd.to_datetime(df["dateChecked"])
        return df

# ------------------------------------------------------------

# --- simple tests
def main():

    ds = DataSource()
    logger.info(f"working\n{ds.working.info()}")
    logger.info(f"history\n{ds.history.info()}")
    logger.info(f"current\n{ds.current.info()}")


if __name__ == '__main__':
    main()
