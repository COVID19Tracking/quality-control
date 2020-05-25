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
import requests
import socket
import io

from app.util import state_abbrevs
import app.util.udatetime as udatetime
from app.data.worksheet_wrapper import WorksheetWrapper
from app.log.error_log import ErrorLog

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
KEY_PATH = "credentials-scanner.json"

def get_remote_csv(xurl: str) -> pd.DataFrame:
    r = requests.get(xurl, timeout=1)
    if r.status_code >= 300:
        raise Exception(f"Could not get {xurl}, status={r.status_code}")
    f = io.StringIO(r.text)
    df = pd.read_csv(f)
    return df



class DataSource:

    def __init__(self):

        self._target_date = None
        self.log = ErrorLog()

        self.failed = {}

        # worksheet dates
        self.last_publish_time = ""
        self.last_push_time = ""
        self.current_time = ""

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
            except socket.timeout:
                self.failed["working"] = True
                self.log.error(f"Could not fetch working")
            except Exception as ex:
                logger.exception(ex)
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
            except socket.timeout:
                self.failed["history"] = True
                self.log.error(f"Could not fetch history")
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
            except socket.timeout:
                self.failed["current"] = True
                self.log.error(f"Could not fetch current")
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
            except socket.timeout:
                self.failed["CDS"] = True
                self.log.warning(f"Could not fetch CDS counties")
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
            except socket.timeout:
                self.failed["CSBS"] = True
                self.log.warning(f"Could not fetch CSBS counties")
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
            except socket.timeout:
                self.failed["NYT"] = True
                self.log.warning(f"Could not fetch NYT counties")
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
        try:
            s = df[col_name].str.strip().str.replace(",", "")
            df[col_name] = s

            is_blank = (s == "")
            is_bad = (~s.str.isnumeric()) & (~is_blank)

            df.loc[is_blank, col_name] = "-1000"
            s = df[col_name]

            df_errs = s[is_bad]
            if df_errs.shape[0] == 0:
                return s.astype(np.int)

            df_errs = df[["state", col_name]][is_bad]
            logger.error(f"invalid input values for {col_name}:\n{df_errs}")
            for idx, e_row in df_errs.iterrows():
                v = e_row[col_name]
                v2 = s[idx]
                self.log.error(f"Invalid {col_name} value ({v} -> {v2}) for {e_row.state}")

            s = s.where(is_bad, other="-1001")
            return s.astype(np.int)
        except Exception as ex:
            logger.error(f"Cannot convert {col_name} to int: {ex}")
            exit(-1)

    def parse_dates(self, dates: List):
        if len(dates) < 4:
            raise Exception("First row layout (containing dates) changed")
        last_publish_label, last_publish_value, last_push_label, \
            last_push_value = dates[:4]

        # find current time
        current_time_field = None
        for v in dates[4:]:
            if v.startswith("CURRENT TIME:"):
                current_time_field = v
                break

        if last_publish_label != "Last Publish Time:":
            raise Exception("Last Publish Time (cells V1:U1) moved")
        if last_push_label != "Last Push Time:":
            raise Exception("Last Push Time (cells Z1:AA1) moved")
        if current_time_field is None:
            raise Exception("CURRENT TIME (cell AG1) moved")

        self.last_publish_time = last_publish_value
        self.last_push_time = last_push_value
        self.current_time = current_time_field[current_time_field.index(":")+1:].strip()


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
        #    'Private': '',

            'col_18': '',
            'Local Time':'localTime',

            # new
            'Total Antibody Tests (People)': 'antibody_people_total',
            'Positive Antibody Tests (People)': 'antibody_people_pos',
            'Negative Antibody Tests (People)': 'antibody_people_neg',

            'Total Tests (PCR)': 'specimens_total',
            'Positive Tests (PCR)': 'specimens_positive',
            'Negative Tests (PCR)': 'specimens_negative',

            'Positive Cases (PCR)': 'positive',
            'Total Tests (People)': 'total_people',
            'Positive Cases (People, confirmed + probable)': 'positive_probable',
            'Negative (People or Cases)': 'negative',
            'Pending':'pending',
            'Currently Hospitalized':'hospitalized',
            'Currently Hospitalized 1':'hospitalizedFlag',
            'Cumulative Hospitalized':'hospitalizedCumulativeFlag',
            #'Cumulative Hospitalized 1':'hospitalizedCumulativeFlag',
            'Currently in ICU':'inIcu',
            'Currently in ICU 1':'inIcuIsReported',
            'Cumulative in ICU':'inIcuCumulative',
            'Cumulative in ICU 1':'inIcuCumulativeFlag',
            'Currently on Ventilator':'onVentilator',
            'Currently on Ventilator 1':'onVentilatorFlag',
            'Cumulative on Ventilator':'onVentilatorCumulative',
            'Cumulative on Ventilator 1':'onVentilatorCumulativeFlag',
            'Recovered':'recoveredFlag',

            'Deaths (confirmed and probable)': 'death',
            'Deaths (confirmed)': 'death_confirmed',
            'Deaths (probable)': 'death_probable',

            'Last Update (ET)': 'lastUpdateEt',
            'Last Check (ET)': 'lastCheckEt',
            'Checker':'checker',
            'Doublechecker':'doubleChecker',

            'Notes': '',

            # hidden
            'Positive': '',
            'Negative': '',
            'Deaths': '',
            'Doublecheck Flag': '',
            'Reporting Negatives': '',
            'Hospitalized – Currently': '',
            'Hospitalized – Cumulative': '',
            'In ICU – Currently': '',
            'In ICU – Cumulative': '',
            'On Ventilator – Currently': '',
            'On Ventilator – Cumulative': '',
            'pubDate': '',
            'pushDate': '',
            # end hidden

            'currentlyHospitalizedFlag': '',
            'cumulativeHospitalizedFlag': '',
            'currentICUFlag': '',
            'cumulativeICUFlag': '',
            'currentVentilatorFlag': '',
            'cumulativeVentilatorFlag': '',
            'recoveredFlag': '',
            'stateGrade': 'grade',
        }


        gs = WorksheetWrapper()
        dev_id = gs.get_sheet_id_by_name("dev")

        dates = gs.read_as_list(dev_id, "Worksheet 2!W1:BT1", ignore_blank_cells=True, single_row=True)
        self.parse_dates(dates)

        df = gs.read_as_frame(dev_id, "Worksheet 2!A2:BR60", header_rows=1)

        #for i, x in enumerate(df.columns):
        #    logger.info(f"column {i} {x}: {df[x].values[0:5]}")

        # check for duplicate output names
        logger.info("check for duplicate output names")
        has_dups = False
        dups = {}
        for x1 in column_map:
            y = column_map[x1]
            if y == "": continue
            x2 = dups.get(y)
            if x2 != None:
                has_dups = True
                logger.error(f"Duplicate output name {y} for {x1} and {x2}")
            else:
                dups[y] = x1
        if has_dups: raise Exception("Duplicate output names")

        # clean up names
        logger.info("clean up names")
        cols = []
        dup_cnt = {}
        for i, n in enumerate(df.columns):
            n1 = n.replace("\r", "").replace("\n", " ").replace("  ", " ")
            n1 = n1.strip()
            if n1 == "": n1 = f"col_{i}"
            x = dup_cnt.get(n1)
            if x is None:
                dup_cnt[n1] = 0
            else:
                dup_cnt[n1] = x = x + 1
                n1 = f"{n1} {x}"

            cols.append(n1)
        df.columns = cols


        # copy mapped names to new frame
        #   also for unexpected columns
        logger.info("make new dataframe")
        has_error = False
        df_new = pd.DataFrame()
        for i, n in enumerate(df.columns):
            n2 = column_map.get(n)
            #logger.info(f"{n} {type(n)} -> {n2} {type(n)}")
            if n2 == None:
                has_error = True
                logger.error(f"  Unexpected column {i}: [{n}] in google sheet")
            elif n2 != '':
                if type(df[n]) == pd.Series:
                    df_new[n2] = df[n]
                elif type(df[n]) == pd.DataFrame:
                    logger.error(f"  Name [{n}] matches multiple columns")

        for n in column_map:
            if not (n1 in df.columns):
                has_error = True
                logger.error(f"  Missing column: [{n}] in google sheet")

        if has_error:
            logger.error("Columns in google sheet have changed")
        #    raise Exception("Columns in google sheet have changed")

        df = df_new

        # ---

        logger.info("convert values to int")
        idx = df.columns.get_loc("localTime")
        eidx = df.columns.get_loc("lastUpdateEt")

        for c in df.columns[idx+1:eidx]:
            if c.endswith("Flag"):
                logger.info(f"  {c} is marked as boolean, skipping")
            else:
                df[c] = self.safe_convert_to_int(df, c)

        def standardize(d: str) -> str:
            sd, err_num = udatetime.standardize_date(d)
            return str(err_num) + sd

        def convert_date(df: pd.DataFrame, name: str, as_eastern: bool):
            s = df[name]
            s_date = s.apply(standardize)

            s_idx = s_date.str[0].astype(np.int)
            names = ["", "changed", "blank", "missing date", "missing time", "bad date", "bad time"]
            s_msg = s_idx.map(lambda x: names[x])

            s_date = s_date.str[1:]

            #print(pd.DataFrame({ "before": s, "after": s_date, "changed": s_changed}))

            s_date = pd.to_datetime(s_date, format="%m/%d/%Y %H:%M")
            if as_eastern:
                s_date = s_date.apply(udatetime.pandas_timestamp_as_eastern)

            df[name] = s_date
            df[name + "_msg"] = s_msg

        # remove current time from first row
        #current_time = df.loc[0, "lastCheckEt"].replace("CURRENT NAME: ", "")
        #df.loc[0, "lastCheckEt"] = ""

        logger.info("convert dates")
        convert_date(df, "localTime", as_eastern=False)
        convert_date(df, "lastUpdateEt", as_eastern=True)
        convert_date(df, "lastCheckEt", as_eastern=True)

        df = df[ df.state != ""]


        logger.info("compute total (used by fit checks)")
        df["total"] = df["positive"] + df["negative"]
        return df

    def load_current(self) -> pd.DataFrame:
        """ load the current values from the API """

        df = get_remote_csv("https://covidtracking.com/api/states.csv")

        df = df.fillna(0)
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
        for c in ['hospitalizedCumulative', 'inIcuCumulative', 'onVentilatorCumulative']:
            df[c] = df[c].astype(np.int)

        # 0 or 1.  score = sum of others so it is 0-4
        for c in ["positiveScore", "negativeScore", "negativeRegularScore", "commercialScore", "score"]:
            df[c] = df[c].astype(np.int)
        return df


    def load_history(self) -> pd.DataFrame:
        """ load daily values over time from the API """

        df = get_remote_csv("https://covidtracking.com/api/states/daily.csv")
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

        cds = get_remote_csv("https://coronadatascraper.com/data.csv")

        cds = cds \
            .loc[(cds["country"] == "USA") & (~cds["county"].isnull())]

        cds["county"] = cds["county"].apply(lambda x: x.replace("County", "").strip())
        cds["source"] = "cds"
        return cds

    def load_csbs_counties(self) -> pd.DataFrame:
        """ load the CSBS county dataset """

        xurl = "http://coronavirus-tracker-api.herokuapp.com/v2/locations?source=csbs"
        response = urlopen(xurl, timeout=1)
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

        df = get_remote_csv("https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv")

        """ load the NYT county dataset """
        nyt = df.rename(columns={
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
