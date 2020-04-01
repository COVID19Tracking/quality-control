#
# plot support for forecast
#
#   plotting is resource intensive so extract it from
#   the normal code.
#

import warnings
warnings.filterwarnings('ignore')

import os
from datetime import datetime, date, timedelta
import pandas as pd
import numpy as np
from loguru import logger

import matplotlib
import matplotlib.pyplot as plt

from forecast import Forecast, _exp_fit, _linear_fit

g_first_time = True
matplotlib.style.use('fivethirtyeight')

def _format_date(date:int) -> str:
    """return YYYYmmdd as YYYY-mm-dd"""
    str_date = str(date)
    return f"{date[:4]}-{date[4:6]}-{date[6:]}"

def plot_to_file(forecast: Forecast, image_dir: str, fit_thresholds: list):

    global g_first_time
    if g_first_time:
        logger.debug("  [plot forecast]")
        g_first_time = False

    if forecast is None:
        raise Exception("Missing forecast")

    to_plot = forecast.cases_df[["index", "positive"]].append(
        {"index":forecast.projection_index, "positive":forecast.actual_value}, ignore_index=True)
    exp_fit = _exp_fit(to_plot["index"], *forecast.fitted_exp_params)
    linear_fit = _linear_fit(to_plot["index"], *forecast.fitted_linear_params)



    plt.figure(figsize=(9,15))

    ax = to_plot.plot.bar(x="index", y="positive", color="gray", alpha=.7, label="actual positives growth")
    plt.plot(to_plot["index"], linear_fit, color="black", label="projected growth")
    plt.plot(to_plot["index"], exp_fit, color="red", label="exponential fit")

    plt.vlines(forecast.projection_index, linear_fit[forecast.projection_index],
        linear_fit[forecast.projection_index]*fit_thresholds[0], colors="black", linestyles="dashed")
    plt.vlines(forecast.projection_index, exp_fit[forecast.projection_index],
        exp_fit[forecast.projection_index]*fit_thresholds[1], colors="red", linestyles="dashed")


    first_datetime = datetime.strptime(str(forecast.cases_df["date"].min()), '%Y%m%d')
    projection_datetime = datetime.strptime(str(forecast.date), '%Y%m%d')
    delta = projection_datetime - first_datetime

    plotted_dates = [(first_datetime + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(delta.days + 1)]

    plt.title(f"{forecast.state} ({forecast.date}): {forecast.actual_value} positives, expected between {forecast.expected_linear} and {forecast.expected_exp}")
    ax.set_xticklabels(plotted_dates, rotation=90)
    plt.xlabel("Day")
    plt.ylabel("Number of positive cases")
    plt.ylim(0, np.ceil(max(forecast.results)*1.2))
    plt.legend()

    # TODO: Might want to save these to s3?
    # This write-to-file step adds ~1 sec of runtime / state

    if not os.path.isdir(image_dir): os.makedirs(image_dir)

    fn = f"predicted_positives_{forecast.state}_{forecast.date}.png"
    plt.savefig(os.path.join(image_dir, fn), dpi=250, bbox_inches = "tight")

