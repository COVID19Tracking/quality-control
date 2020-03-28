# Error Report -- collect the warning/errors for review

class ErrorReport:

    def __init__(self):
        self.warnings = []
        self.errors = []

    def warning(self, location: str, message: str):
        self.warnings.append({"location": location, "message": message })

    def error(self, location: str, message: str):
        self.errors.append({"location": location, "message": message })

    def print(self):

        if len(self.errors) > 0:
            print("==== ERRORS =====")
            for x in self.errors:
                location, message = x["location"], x["message"]
                print(f"  {location}: {message}")

        if len(self.warnings) > 0:
            print("==== ERRORS =====")
            for x in self.warnings:
                location, message = x["location"], x["message"]
                print(f"  {location}: {message}")

