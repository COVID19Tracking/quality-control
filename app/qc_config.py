from datetime import timedelta
from loguru import logger

import app.util.udatetime as udatetime

class QCConfig():
    " configuration options for how to run checks "

    def __init__(self, 
        enable_experimental = False,
        enable_debug = False,
        results_dir = "results",
        images_dir = "images", 
        save_results = False,
        plot_models = False,
        ):

        # checks
        self.results_dir = results_dir # place to store hdf5 files
        self.save_results = save_results # save results to an hdf5 file
        self.enable_experimental = enable_experimental # rerun stuff still in development
        self.enable_debug = enable_debug # turn on tracing

        # forecast
        self.images_dir = images_dir # place to store images
        self.plot_models = plot_models # generate model curves for forecast

        # format
        self.show_dates = False # request more date context in messages 

        # computed
        self.init_publish_date()

    def init_publish_date(self):

        # expect publish at 5PM ET, push at 12AM, 5PM, and 12PM
        #    publish means history updates
        #    push means current updates
        dt = udatetime.now_as_eastern()
        if dt.hour < 8:            
            dt = dt + timedelta(days=-1)
            dt_current = dt_history = dt_working = dt
            push_num = 3
        elif dt.hour < 12:
            dt_current = dt_history = dt + timedelta(days=-1)
            dt_working = dt
            push_num = 3
        elif dt.hour < 5:
            dt_history = dt + timedelta(days=-1)
            dt_current = dt_working = dt
            push_num = 1
        else:
            dt_history = dt + timedelta(days=-1)
            dt_current = dt_working = dt
            push_num = 2


        if 3 <= dt.hour <= 5:
            self.is_near_release = True
        elif 11 <= dt.hour <= 12:
            self.is_near_release = True
        else:
            self.is_near_release = False
        
        # working_date is the date for the spreadsheet  
        self.working_date =  dt_working
        self.working_date_int = dt_working.year * 10000 + dt_working.month * 100 + dt_working.day

        # publish_date is when history has been updated
        self.publish_date =  dt_history
        self.publish_date_int = dt_history.year * 10000 + dt_history.month * 100 + dt_history.day

        # push_date is when current has been updated
        self.push_date =  dt_current
        self.push_date_int = dt_current.year * 10000 + dt_current.month * 100 + dt_current.day

        # which push we are at for the current table
        self.push_num = push_num

        logger.info(f"dates: ")
        logger.info(f" working date is {self.working_date_int}")
        logger.info(f" push  date is {self.push_date_int}")
        logger.info(f" publish date is {self.publish_date_int}")
        logger.info(f" push num is {self.push_num}")

