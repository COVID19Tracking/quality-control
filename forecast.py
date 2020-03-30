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

class ForecastConfig():
    " configuration options for how to run forecast model "

    def __init__(self, images_dir = "images", plot_models = False):
        self.images_dir = images_dir
        self.plot_models = plot_models


class Forecast():
    " simple forecast model for estimating if new values are reasonable "

    def __init__(self):

        self.df: pd.DataFrame = None

        self.state = ""
        self.date = ""
        self.actual_value = 0
        self.expected_exp = 0
        self.expected_linear = 0

        self.case_df: pd.DataFrame = None
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

        self.cases_df = (self.df
            .sort_values("date", ascending=True)
            .reset_index(drop=True)
            .rename_axis('index')
            .reset_index())

        to_fit_exp = self.cases_df
        to_fit_linear = self.cases_df[-4:]

        self.fitted_linear_params = _get_distribution_fit(to_fit_linear["index"], to_fit_linear["positive"], _linear_fit)
        self.fitted_exp_params = _get_distribution_fit(to_fit_exp["index"], to_fit_exp["positive"], _exp_fit)

    def project(self, row: tuple) -> None:
        "Get forecasted positives value for current day"
        self.date = row.lastUpdateEt.to_pydatetime().strftime('%Y-%m-%d')
        self.actual_value = row.positive

        self.projection_index = self.cases_df["index"].max() + \
            (int(self.date.replace("-","")) - self.cases_df["date"].max())
        self.expected_exp = _exp_fit(self.projection_index, *self.fitted_exp_params).round().astype(int)
        self.expected_linear = _linear_fit(self.projection_index, *self.fitted_linear_params).round().astype(int)

    def plot(self, image_dir: str):
        "Plot case growth (expected and actuals)"

        matplotlib.style.use('ggplot')

        plt.figure(figsize=(9,15))
        to_plot = self.cases_df[["index", "positive"]].append(
            {"index":self.projection_index, "positive":self.actual_value}, ignore_index=True)
        ax = to_plot.plot.bar(x="index", y="positive", color="gray", alpha=.7, label="actual positives growth")
        plt.plot(to_plot["index"], _exp_fit(to_plot["index"], *self.fitted_exp_params), color="red", label="exponential fit")
        plt.plot(to_plot["index"], _linear_fit(to_plot["index"], *self.fitted_linear_params), color="black", label="projected growth")

        plotted_dates = [_format_date(str(d)) for d in np.arange(
            self.cases_df.date.min(), int(self.date.replace("-",""))+1)]
        plt.title(f"{self.state} ({self.date}): {self.actual_value} positives, expected between {self.expected_linear} and {self.expected_exp}")
        ax.set_xticklabels(plotted_dates, rotation=90)
        plt.xlabel("Day")
        plt.ylabel("Number of positive cases")
        plt.ylim(0, np.ceil(max(self.results)*1.2))
        plt.legend()

        # TODO: Might want to save these to s3?
        # This write-to-file step adds ~1 sec of runtime / state

        if not os.path.isdir(image_dir): os.makedirs(image_dir)

        fn = f"predicted_positives_{self.state}_{self.date}.png"
        plt.savefig(os.path.join(image_dir, fn), dpi=250, bbox_inches = "tight")
