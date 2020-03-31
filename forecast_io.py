import os
import h5py
import pandas as pd
import numpy as np

from forecast_plot import Forecast

def save_forecast_hd5(forecast: Forecast, data_dir: str):

    fn = f"predicted_positives_{forecast.state}_{forecast.date}.hd5"

    out_path = os.path.join(data_dir, fn)
    tmp_path = out_path + ".tmp"
    if os.path.exists(out_path): os.remove(out_path)
    if os.path.exists(tmp_path): os.remove(tmp_path)

    hf = h5py.File(tmp_path, "w")
    hf.attrs["state"] = forecast.state
    hf.attrs["date"] = forecast.date
    hf.attrs["actual_value"] = forecast.actual_value
    hf.attrs["expected_exp"] = forecast.expected_exp
    hf.attrs["expected_linear"] = forecast.expected_linear
    hf.attrs["projection_index"] = forecast.projection_index
    hf.close()

    forecast.df.to_hdf(tmp_path, "df")
    forecast.cases_df.to_hdf(tmp_path, "cases_df")

    df_pars = pd.DataFrame({
        "linear": forecast.fitted_linear_params,
        "exp": forecast.fitted_exp_params  
    })
    df_pars.to_hdf(tmp_path, "fitted_params")
        
    #h5_fout.create_dataset(
    #    name='labels',
    #    data=labels,
    #    compression='gzip', compression_opts=4,
    #    dtype=label_dtype)

    
    os.rename(tmp_path, out_path)

def load_forecast_hd5(data_dir: str, state: str, date: int) -> Forecast:

    fn = f"predicted_positives_{state}_{date}.hd5"

    path = os.path.join(data_dir, fn)
    if not os.path.exists(path): return None

    forecast = Forecast()

    hf = h5py.File(path, "r")
    forecast.state = hf.attrs["state"]
    forecast.date = hf.attrs["date"]
    forecast.actual_value = hf.attrs["actual_value"]
    forecast.expected_exp = hf.attrs["expected_exp"]
    forecast.expected_linear = hf.attrs["expected_linear"]
    forecast.projection_index = hf.attrs["projection_index"]
    hf.close()

    forecast.df = pd.read_hdf(path, "df")
    forecast.cases_df = pd.read_hdf(path, "cases_df")
    
    df_pars = pd.read_hdf(path, "fitted_params")
    forecast.fitted_linear_params = df_pars.linear.values
    forecast.fitted_exp_params = df_pars.exp.values
     
    return forecast

def test():

    forecast = Forecast()
    forecast.state = "AZ"
    forecast.date = 20200101
    forecast.actual_value = 1000
    forecast.expected_exp = 1002
    forecast.expected_linear = 999

    forecast.df = pd.DataFrame({"fake": [ 0, 1, 2, 4]})
    forecast.cases_df = pd.DataFrame({"fake": [ 0, 1, 2, 4]})
    forecast.projection_index = 0
    forecast.fitted_linear_params = np.array([ 0, 1,])
    forecast.fitted_exp_params = np.array([ 0, 1,])

    save_forecast_hd5(forecast, "results")
    f2 = load_forecast_hd5("results", forecast.state, forecast.date)

if __name__ == "__main__":
    test()