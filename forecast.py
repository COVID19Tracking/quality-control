#
# Forecast -- a simple model of how many cases are excepted given recent data
#
import os
import pandas as pd
import numpy as np
from scipy.optimize import curve_fit
import matplotlib
import matplotlib.pyplot as plt
import warnings
from typing import Tuple

warnings.filterwarnings('ignore')

def _format_date(date:str) -> str:
    """return YYYYmmdd as YYYY-mm-dd"""
    return f"{date[:4]}-{date[4:6]}-{date[6:]}"

def _exp_curve(x: float, a: float, b: float) -> float:
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

    def __init__(self):
        self.df: pd.DataFrame = None        
        
        self.state = ""
        self.date = ""
        self.actual_value = 0
        self.expected_exp = 0
        self.expected_linear = 0

        self.case_df: pd.DataFrame = None        
        self.fitted_linear = None
        self.fitted_exp = None

    def fit(self, df: pd.DataFrame):
        "Fit an exponential and linear model to the history"
        self.df = df        
        self.state = df["state"].values[0]

        cases_df = (self.df
            .sort_values("date", ascending=True)
            .reset_index(drop=True)
            .rename_axis('index')
            .reset_index())

        to_fit_exp = cases_df[:-1]
        to_fit_linear = cases_df[-5:-1]
        to_forecast = cases_df.tail(1)

        self.fitted_linear = _get_distribution_fit(to_fit_linear["index"], to_fit_linear["positive"], _linear_fit)
        self.fitted_exp = _get_distribution_fit(to_fit_exp["index"], to_fit_exp["positive"], _exp_curve)

        self.cases_df = cases_df

        self.date = _format_date(to_forecast["date"].values[0].astype(str))
        self.actual_value = to_forecast["positive"].values[0]
        self.expected_exp = _exp_curve(to_forecast["index"].values[0], *self.fitted_exp).round().astype(int)
        self.expected_linear = _linear_fit(to_forecast["index"].values[0], *self.fitted_linear).round().astype(int)

    @property
    def results(self) -> Tuple[int, int, int]:
        "Get the results from the model"
        return self.actual_value, self.expected_linear, self.expected_exp

    def plot(self, image_dir: str):
        "Plot case growth (expected and actuals)"

        matplotlib.style.use('ggplot')

        cases_df = self.cases_df

        plt.figure(figsize=(9,15))
        ax = cases_df.plot.bar(x="index", y="positive", color="gray", alpha=.7, label="actual positives growth")
        plt.plot(cases_df["index"], _exp_curve(cases_df["index"], *self.fitted_exp), color="red", label="exponential fit")
        plt.plot(cases_df["index"], _linear_fit(cases_df["index"], *self.fitted_linear), color="black", label="projected growth")

        plt.title(f"{self.state} ({self.date}): Expected {self.expected_linear}, got {self.actual_value}")
        ax.set_xticklabels(cases_df["date"].apply(lambda d: _format_date(str(d))), rotation=90)
        plt.xlabel("Day")
        plt.ylabel("Number of positive cases")
        plt.ylim(0,max(cases_df["positive"].max(), self.expected_exp, self.expected_linear)+10)
        plt.legend()

        # TODO: Might want to save these to s3?
        # This write-to-file step adds ~1 sec of runtime / state
        fn = f"predicted_positives_{self.state}_{self.date}.png"
        plt.savefig(os.path.join(image_dir, fn), dpi=250, bbox_inches = "tight")
