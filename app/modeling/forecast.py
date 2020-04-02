#
# Forecast -- a simple model of how many cases are excepted given recent data
#
import os
from datetime import datetime
import pandas as pd
import numpy as np
from scipy.optimize import curve_fit
from typing import Tuple


def _exp_fit(x: float, a: float, b: float) -> float:
    return a * np.exp(b * x)

def _linear_fit(x: float, m: float, b: float) -> float:
    return m*x + b

def _get_distribution_fit(x: pd.Series, y: pd.Series, dist_func) -> np.array:

    np.random.seed(1729)

    x = np.array(x.values, dtype=float)
    y = np.array(y.values, dtype=float)

    popt, pcov = curve_fit(dist_func, x, y, p0=(4, 0.1))
    return popt


class Forecast():
    " simple forecast model for estimating if new values are reasonable "

    def __init__(self):

        self.df: pd.DataFrame = None

        self.state = ""
        self.date = 0
        self.actual_value = 0
        self.expected_exp = 0
        self.expected_linear = 0

        self.cases_df: pd.DataFrame = None
        self.projection_index = None
        self.fitted_linear_params = None
        self.fitted_exp_params = None



    @property
    def results(self) -> Tuple[int, int, int]:
        "Get the results from the model"
        return self.actual_value, self.expected_linear, self.expected_exp


    def fit(self, df: pd.DataFrame):
        "Fit an exponential and linear model to the history"

        self.df = df
        self.state = df["state"].values[0]

        self.cases_df = self.df \
            .sort_values("date", ascending=True) \
            .reset_index(drop=True) \
            .rename_axis('index') \
            .reset_index()

        to_fit_exp = self.cases_df
        to_fit_linear = self.cases_df[-4:]

        self.fitted_linear_params = _get_distribution_fit(to_fit_linear["index"], to_fit_linear["positive"], _linear_fit)
        self.fitted_exp_params = _get_distribution_fit(to_fit_exp["index"], to_fit_exp["positive"], _exp_fit)

    def project(self, row: tuple) -> None:
        "Get forecasted positives value for current day"
        self.actual_value = row.positive

        prev_datetime = datetime.strptime(str(self.cases_df["date"].max()), '%Y%m%d')
        projection_datetime = datetime.strptime(str(self.date), '%Y%m%d')
        days_forward = (projection_datetime - prev_datetime).days

        self.projection_index = self.cases_df["index"].max() + days_forward
        self.expected_exp = _exp_fit(self.projection_index, *self.fitted_exp_params).round().astype(int)
        self.expected_linear = _linear_fit(self.projection_index, *self.fitted_linear_params).round().astype(int)

