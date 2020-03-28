# ResultLog -- collects results for a run and handle printing/posting them

class ResultLog():

    def __init__(self):
        self.errors = []
        self.warnings = []

    def add_results(self, location: str, error: str, warning: str) -> None:
        if error != None:
            self.errors.append((location, error))
        if warning != None:
            self.warnings.append((location, warning))

    def print(self):

        if len(self.errors) == 0 and len(self.warnings) == 0:
            print("No Errors or Warnings")

        if len(self.errors) > 0:
            print("=====| ERRORS |===========")
            for location, message in self.errors:                
                print(f"{location}: {message}")
        if len(self.warnings) > 0:
            print("=====| WARNINGS |===========")
            for location, message in self.warnings:
                print(f"{location}: {message}")


    def post(self):
        # TODO: Post the result somewhere that people can see it.
        pass