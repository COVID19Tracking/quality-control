
class QCConfig():
    " configuration options for how to run checks "

    def __init__(self, 
        enable_counties = False,
        images_dir = "images", 
        plot_models = False,
        ):

        self.enable_counties = enable_counties

        self.images_dir = images_dir
        self.plot_models = plot_models
