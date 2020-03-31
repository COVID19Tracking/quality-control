# ResultLog -- collects results for a run and handle printing/posting them
import json
import io
import pandas as pd
import numpy as np
import udatetime

class ResultLog():

    def __init__(self):
        self.loaded_at = udatetime.now_as_eastern()

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


    def to_json(self) -> str:

        result = {
            "infomation": [ { "location": x[0], "message": x[1] } for x in self._infos ],
            "warning": [ { "location": x[0], "message": x[1] } for x in self._warnings ],
            "error": [ { "location": x[0], "message": x[1] } for x in self._errors ],
        }
        return json.dumps(result, indent=2)


    def to_frame(self) -> pd.DataFrame:
 
        n_total = len(self._infos) + len(self._warnings) + len(self._errors)
        level = np.zeros(n_total, dtype=object)
        location = np.zeros(n_total, dtype=object)
        message = np.zeros(n_total, dtype=object)

        idx = 0
        for loc, msg in self._errors:
            level[idx], location[idx], message[idx] = "ERROR", loc, msg
            idx += 1  
        for loc, msg in self._warnings:
            level[idx], location[idx], message[idx] = "WARNING", loc, msg
            idx += 1  
        for loc, msg in self._infos:
            level[idx], location[idx], message[idx] = "INFO", loc, msg
            idx += 1  

        df = pd.DataFrame({
            "level": level, "location": location, "message": message
        })
        return df

    def to_csv(self) -> str:
        df = self.to_frame()
        dest = io.StringIO()
        df.to_csv(dest, index=False)
        return dest.getvalue()
    
    def to_html(self) -> str:
        df = self.to_frame()

        # didn't have time to finish - Josh
        # want a css class per row
        #
        # # https://towardsdatascience.com/style-pandas-dataframe-like-a-master-6b02bf6468b0
        #def row_class(r):
        #     c = f"class: " + r.level.lower()
        #     return [c,c,c]
        #result = df.style.apply(row_class, axis=1).render() 

        result = df.to_html()
        return result


# -----------------------------
def test():

    log = ResultLog()
    log.error("NY", "Looking kinda scary.  > 50K")
    log.warning("TX", "We're next, soon")
    log.info("FA", '"Let''s Ignore It"')

    print("--- print ----")
    log.print()

    print("--- to_frame ----")
    df = log.to_frame()
    print(df)

    print("--- to_csv ----")
    s = log.to_csv()
    print(s)

    print("--- to_json ----")
    s = log.to_json()
    print(s)

    print("--- to_html ----")
    s = log.to_html()
    print(s)


if __name__ == "__main__":
    test()
