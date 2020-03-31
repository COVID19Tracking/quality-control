
class QCConfig():
    " configuration options for how to run checks "

    def __init__(self, 
        enable_experimental = False,
        results_dir = "results",
        images_dir = "images", 
        save_results = False,
        plot_models = False,
        ):

        # checks
        self.results_dir = results_dir # place to store hdf5 files
        self.save_results = save_results # save results to an hdf5 file
        self.enable_experimental = enable_experimental # rerun stuff still in development

        # forecast
        self.images_dir = images_dir # place to store images
        self.plot_models = plot_models # generate model curves for forecast

        # format
        self.show_dates = False # request more date context in messages 
