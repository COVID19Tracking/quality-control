# ResultLog -- collects results for a run and handle printing/posting them
import json
import io
import pandas as pd
import numpy as np
import udatetime
from typing import Tuple
import time

class ResultLog():

    def __init__(self):
        self.loaded_at = udatetime.now_as_eastern()
        self.start = time.process_time_ns()

        self._infos = []
        self._warnings = []
        self._errors = []

    def _make(self, location: str, message: str) -> Tuple[str, str, int]:
        end = time.process_time_ns()
        delta_ms = int((end - self.start) * 1e-6)
        self.start = end
        return (location, message, delta_ms)

    def info(self, location: str, message: str) -> None:
        if message is None: raise Exception("Missing message")
        self._infos.append(self._make(location, message))

    def warning(self, location: str, message: str) -> None:
        if message is None: raise Exception("Missing message")
        self._warnings.append(self._make(location, message))

    def error(self, location: str, message: str) -> None:
        if message is None: raise Exception("Missing message")
        self._errors.append(self._make(location, message))

    # -----

    def print(self):

        print("")

        if len(self._errors) == 0 and len(self._warnings) == 0 and len(self._infos) == 0:
            print("[No Messages]")

        def format_ms(ms: int):
            return f" ({ms}ms)" if ms > 100 else ""

        if len(self._errors) > 0:
            print("=====| ERROR |===========")
            for location, message, ms in self._errors:
                print(f"{location}: {message}{format_ms(ms)}")

        if len(self._warnings) > 0:
            print("\n=====| WARNING |===========")
            for location, message, ms in self._warnings:
                print(f"{location}: {message}{format_ms(ms)}")

        if len(self._infos) > 0:
            print("\n=====| INFO |===========")
            for location, message, ms in self._infos:
                print(f"{location}: {message}{format_ms(ms)}")

        print("")


    def to_json(self) -> str:

        result = {
            "infomation": [ { "location": x[0], "message": x[1], "ms": x[2] } for x in self._infos ],
            "warning": [ { "location": x[0], "message": x[1], "ms": x[2] } for x in self._warnings ],
            "error": [ { "location": x[0], "message": x[1], "ms": x[2] } for x in self._errors ],
        }
        return json.dumps(result, indent=2)


    def to_frame(self) -> pd.DataFrame:
 
        n_total = len(self._infos) + len(self._warnings) + len(self._errors)
        level = np.zeros(n_total, dtype=object)
        location = np.zeros(n_total, dtype=object)
        message = np.zeros(n_total, dtype=object)
        time_ms = np.zeros(n_total, dtype=np.int)

        idx = 0
        for loc, msg, ms in self._errors:
            level[idx], location[idx], message[idx], time_ms[idx] = "ERROR", loc, msg, ms
            idx += 1  
        for loc, msg, ms in self._warnings:
            level[idx], location[idx], message[idx], time_ms[idx] = "WARNING", loc, msg, ms
            idx += 1  
        for loc, msg, ms in self._infos:
            level[idx], location[idx], message[idx], time_ms[idx] = "INFO", loc, msg, ms
            idx += 1  

        df = pd.DataFrame({
            "level": level, "location": location, "message": message, "ms": time_ms
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
