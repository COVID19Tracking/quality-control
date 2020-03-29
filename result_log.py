# ResultLog -- collects results for a run and handle printing/posting them

class ResultLog():

    def __init__(self):
        self._infos = []
        self._warnings = []
        self._errors = []

    def info(self, location: str, message: str) -> None:
        if message is None: raise Exception("Missing message")
        self._infos.append((location, message))

    def warning(self, location: str, message: str) -> None:
        if message is None: raise Exception("Missing message")
        self._warnings.append((location, message))

    def error(self, location: str, message: str) -> None:
        if message is None: raise Exception("Missing message")
        self._errors.append((location, message))

    # -----

    def print(self):

        print("")

        if len(self._errors) == 0 and len(self._warnings) == 0 and len(self._infos) == 0:
            print("[No Messages]")

        if len(self._errors) > 0:
            print("=====| ERROR |===========")
            for location, message in self._errors:
                print(f"{location}: {message}")

        if len(self._warnings) > 0:
            print("\n=====| WARNING |===========")
            for location, message in self._warnings:
                print(f"{location}: {message}")

        if len(self._infos) > 0:
            print("\n=====| INFO |===========")
            for location, message in self._infos:
                print(f"{location}: {message}")

        print("")


    def post(self):
        # TODO: Post the result somewhere that people can see it.
        pass
