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
from urllib.request import urlopen
import json
import numpy as np
import re

from app.util import state_abbrevs
import app.util.udatetime as udatetime
from app.data.worksheet_wrapper import WorksheetWrapper
from app.log.error_log import ErrorLog

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
KEY_PATH = "credentials-scanner.json"


class DataSource:

    def __init__(self):

        self._target_date = None
        self.log = ErrorLog()

        self.failed = {}

        # internal datasources
        self._working: pd.DataFrame = None
        self._history: pd.DataFrame = None
        self._current: pd.DataFrame = None

        # external datasources
        self._cds_counties: pd.DataFrame = None
        self._csbs_counties: pd.DataFrame = None
        self._nyt_counties: pd.DataFrame = None
        self._county_rollup: pd.DataFrame = None

    @property
    def working(self) -> pd.DataFrame:
        " the working dataset"
        if self._working is None:
            if self.failed.get("working"): return None
            try:
                self._working = self.load_working()
            except Exception as ex:
                self.failed["working"] = True
                self.log.error(f"Could not load working", exception=ex)
        return self._working

    @property
    def history(self) -> pd.DataFrame:
        " the daily history dataset"
        if self._history is None:
            if self.failed.get("history"): return None
            try:
                self._history = self.load_history()
            except Exception as ex:
                self.failed["history"] = True
                self.log.error(f"Could not load history", exception=ex)
        return self._history

    @property
    def current(self) -> pd.DataFrame:
        " today's dataset"
        if self._current is None:
            if self.failed.get("current"): return None
            try:
                self._current = self.load_current()
            except Exception as ex:
                self.failed["current"] = True
                self.log.error("Could not load current", exception=ex)
        return self._current

    @property
    def cds_counties(self) -> pd.DataFrame:
        " the CDS counties dataset"
        if self._cds_counties is None:
            if self.failed.get("CDS"): return None
            try:
                self._cds_counties = self.load_cds_counties()
            except Exception as ex:
                self.failed["CDS"] = True
                self.log.warning("Could not load CDS counties", exception=ex)
        return self._cds_counties

    @property
    def csbs_counties(self) -> pd.DataFrame:
        " the CSBS counties dataset"
        if self._csbs_counties is None:
            if self.failed.get("CSBS"): return None
            try:
                self._csbs_counties = self.load_csbs_counties()
            except Exception as ex:
                self.failed["CSBS"] = True
                self.log.warning(f"Could not load CSBS counties", exception=ex)
        return self._csbs_counties

    @property
    def nyt_counties(self) -> pd.DataFrame:
        " the NYT counties dataset"
        if self._nyt_counties is None:
            if self.failed.get("NYT"): return None
            try:
                self._nyt_counties = self.load_nyt_counties()
            except Exception as ex:
                self.failed["NYT"] = True
                self.log.warning(f"Could not load NYT counties", exception=ex)
        return self._nyt_counties

    @property
    def county_rollup(self) -> pd.DataFrame:
        """ return a single county dataset of select metrics """

        metrics = ["cases", "deaths","recovered"]

        if self._county_rollup is None:
            if len(self.failed) > 0: return None

            frames = [self.cds_counties, self.csbs_counties, self.nyt_counties]
            if self.log.has_error:
                self.failed["counties"] = True
                logger.warning("Could not load datasets for " + ",".join(self.failed))
                return None

            try:
                long_df = pd.concat(frames, axis=0, sort=False)

                self._county_rollup = long_df \
                    .groupby(["state", "source"])[metrics] \
                    .sum() \
                    .fillna(0) \
                    .astype(int) \
                    .reset_index()
            except Exception as ex:
                self.log.warning(f"Could not combine counties datasets: {ex}")

        return self._county_rollup

    def safe_convert_to_int(self, df: pd.DataFrame, col_name: str) -> pd.Series:
        " convert a series to int even if it contains bad data"
        s = df[col_name].str.strip().replace("", "0").replace(re.compile(","), "")

        flags = s.str.isnumeric()
        df_errs = df[~flags]
        if df_errs.shape[0] == 0: return s.astype(np.int)

        df_errs = df_errs[["state", col_name]]
        logger.error(f"invalid input values for {col_name}:\n{df_errs}")
        for _, e_row in df_errs.iterrows():
            print(e_row)
            v = e_row[col_name]
            self.log.error(f"Invalid {col_name} value ({v}) for {e_row.state}")

        s = s.where(flags, other="-1000")
        return s.astype(np.int)



    def load_working(self) -> pd.DataFrame:
        """Load the working (unpublished) data from google sheets"""

        # make dev columns match api columns so quality
        # checks run with both inputs
        column_map = {
            'State':'state',

            'Dashboard': '',
            'State Name': '',
            'State COVID-19 Page': '',
            'State Social Media': '',
            'State Social Media': '',
            'Press Conferences': '',
            'GIS Query': '',
            'Other': '',
            '#Reporting': '',
            'URL Watch': '',
            'Status': '',
            'URL Watch Diff': '',
            'Alerted': '',
            'Last Alert': '',
            'Error': '',
            'Prev Last Check (ET)': '',
            'Freshness': '',
            'Flagged': '',
            'Time zone +/–': '',
            'Public': '',
            '': '',
        #    'Private': '',

            'Local Time':'localTime',
            'Positive':'positive',
            'Negative':'negative',
            'Pending':'pending',
            'Currently Hospitalized':'hospitalized',
            'Cumulative Hospitalized':'hospitalizedCumulative',
            'Currently in ICU':'icu',
            'Cumulative in ICU':'inIcuCumulative',
            'Currently on Ventilator':'ventilator',
            'Cumulative on Ventilator':'onVentilatorCumulative',
            'Recovered':'recovered',
            'Deaths':'death',
            'Total':'total',
            'Last Update (ET)': 'lastUpdateEt',
            'Last Check (ET)': 'lastCheckEt',
            'Checker':'checker',
            'Doublechecker':'doubleChecker'
        }

        gs = WorksheetWrapper()
        dev_id = gs.get_sheet_id_by_name("dev")
        df = gs.read_as_frame(dev_id, "Worksheet 2!A2:AL60", header_rows=1)

        # check names and rename/suppress columns
        has_error = False
        names = []
        to_delete = []
        for n in df.columns.values:
            n2 = column_map.get(n)
            if n2 == None:
                has_error = True
                logger.error(f"  Unexpected column: [{n}] in google sheet")
            if n2 == '':
                to_delete.append(n)
            else:
                names.append(n2)
        for n in column_map:
            if not (n in df.columns):
                has_error = True
                logger.error(f"  Missing column: [{n}] in google sheet")

        if has_error:
            raise Exception("Columns in google have changed")

        for n in to_delete:
            del df[n]

        df.columns = names

        idx = df.columns.get_loc("localTime")
        eidx = df.columns.get_loc("lastUpdateEt")

        for c in df.columns[idx+1:eidx]:
            df[c] = self.safe_convert_to_int(df, c)

        def standardize(d: str) -> str:
            sd, changed = udatetime.standardize_date(d)
            if changed:
                return "1" + sd
            else:
                return "0" + sd

        def convert_date(df: pd.DataFrame, name: str, as_eastern: bool):
            s = df[name]
            s_date = s.apply(standardize)
            s_changed = s_date.str[0]
            s_date = s_date.str[1:]

            #print(pd.DataFrame({ "before": s, "after": s_date, "changed": s_changed}))

            s_date = pd.to_datetime(s_date, format="%m/%d/%Y %H:%M")
            if as_eastern:
                s_date = s_date.apply(udatetime.pandas_timestamp_as_eastern)


            df[name] = s_date
            df[name + "_reformated"] = s_changed.values == "1"

        # remove current time from first row
        #current_time = df.loc[0, "lastCheckEt"].replace("CURRENT NAME: ", "")
        #df.loc[0, "lastCheckEt"] = ""

        convert_date(df, "localTime", as_eastern=False)
        convert_date(df, "lastUpdateEt", as_eastern=True)
        convert_date(df, "lastCheckEt", as_eastern=True)

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
        for c in ["positive", "negative", "pending", "hospitalized", "death", "recovered", "total", "totalTestResults"]:
            df[c] = df[c].astype(np.int)
        for c in ["positiveIncrease", "negativeIncrease", "hospitalizedIncrease", "deathIncrease", "totalTestResultsIncrease"]:
            df[c] = df[c].astype(np.int)
        for c in ['hospitalizedCumulative', 'inIcuCumulative', 'onVentilatorCumulative']:
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
        for c in ["positive", "negative", "pending", "hospitalized", "death", "recovered", "total", "totalTestResults"]:
            df[c] = df[c].astype(np.int)
        for c in ["positiveIncrease", "negativeIncrease", "hospitalizedIncrease", "deathIncrease", "totalTestResultsIncrease"]:
            df[c] = df[c].astype(np.int)
        for c in ['hospitalizedCumulative', 'inIcuCumulative', 'onVentilatorCumulative']:
            df[c] = df[c].astype(np.int)


        df["dateChecked"] = pd.to_datetime(df["dateChecked"])
        return df

    def load_cds_counties(self) -> pd.DataFrame:
        """ load the CDS county dataset """
        cds = pd.read_csv("https://coronadatascraper.com/data.csv")
        cds = cds \
            .loc[(cds["country"] == "USA") & (~cds["county"].isnull())]

        cds["county"] = cds["county"].apply(lambda x: x.replace("County", "").strip())
        cds["source"] = "cds"
        return cds

    def load_csbs_counties(self) -> pd.DataFrame:
        """ load the CSBS county dataset """
        response = urlopen("http://coronavirus-tracker-api.herokuapp.com/v2/locations?source=csbs")
        json_data = response.read().decode('utf-8', 'replace')
        d = json.loads(json_data)
        csbs = pd.json_normalize(d['locations'])

        # remove "extras"
        csbs = csbs \
            .loc[csbs["country"] == "US"] \
            .rename(columns={
                "province":"state",
                "latest.confirmed":"cases",
                "latest.deaths":"deaths",
                "latest.recovered":"recovered",
                "coordinates.latitude":"lat",
                "coordinates.longitude":"long"})
        csbs["state"] = csbs["state"].map(state_abbrevs)
        csbs["source"] = "csbs"
        return csbs

    def load_nyt_counties(self) -> pd.DataFrame:
        """ load the NYT county dataset """
        nyt = pd.read_csv("https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv") \
            .rename(columns={
                "date":"last_updated"
            })
        nyt = nyt.loc[nyt["last_updated"] == nyt["last_updated"].max()]
        nyt["state"] = nyt["state"].map(state_abbrevs)
        nyt["source"] = "nyt"
        return nyt

# ------------------------------------------------------------

# --- simple tests
def main():

    ds = DataSource()
    logger.info(f"working\n{ds.working.info()}")
    logger.info(f"history\n{ds.history.info()}")
    logger.info(f"current\n{ds.current.info()}")


if __name__ == '__main__':
    main()
