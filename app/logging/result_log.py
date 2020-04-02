# ResultLog -- collects results for a run and handle printing/posting them
#
# Each message is assigned a category
#
from enum import Enum
import json
import io
import pandas as pd
import numpy as np
from typing import Tuple, Dict, List
import time
import html

from app.util import udatetime

class ResultCategory(Enum):
    DATA_QUALITY = "data quality"
    DATA_SOURCE = "data source"
    OPERATIONAL = "operations"
    INTERNAL_ERROR = "internal error"

class ResultMessage:

    __slots__ = (
        'category',
        'location',
        'message',
        'ms'
    )

    def __init__(self, category: ResultCategory, location: str, message: str, ms: int):
        self.category = category
        self.location = location
        self.message = message
        self.ms = ms

    def to_dict(self) -> Dict:
        return { "category": self.category.value, "location": self.location, "message": self.message, "ms": self.ms }

class ResultLog():

    def __init__(self):
        self.loaded_at = udatetime.now_as_eastern()
        self.start = time.process_time_ns()

        self._messages: List[ResultLog] = []

    @property
    def messages(self) -> List[ResultMessage]:
        return self._messages

    def by_category(self, category: ResultCategory) -> List[ResultMessage]:
        return [x for x in self._messages if x.category == category]

    def add(self, category: ResultCategory, location: str, message: str) -> None:
        if message is None: raise Exception("Missing message")

        end = time.process_time_ns()
        delta_ms = int((end - self.start) * 1e-6)
        self.start = end

        msg = ResultMessage(category, location, message, delta_ms)
        self._messages.append(msg)

    #def error(self, location: str, message: str) -> None:
    #    self.add(ResultCategory.ERROR, location, message)
    #def warning(self, location: str, message: str) -> None:
    #    self.add(ResultCategory.WARNING, location, message)
    #def info(self, location: str, message: str) -> None:
    #    self.add(ResultCategory.INFO, location, message)

    def operational(self, location: str, message: str) -> None:
        self.add(ResultCategory.OPERATIONAL, location, message)
    def data_quality(self, location: str, message: str) -> None:
        self.add(ResultCategory.DATA_QUALITY, location, message)
    def data_source(self, location: str, message: str) -> None:
        self.add(ResultCategory.DATA_SOURCE, location, message)
    def internal_error(self, location: str, message: str) -> None:
        self.add(ResultCategory.INTERNAL_ERROR, location, message)

    # -----

    def print(self):

        print("")

        if len(self._messages) == 0:
            print("[No Messages]")

        for cat in ResultCategory:
            messages = self.by_category(cat)
            if len(messages) == 0: continue

            print(f"=====| {cat.value.upper()} |===========")
            for x in messages:
                print(f"{x.location}: {x.message}")

        print("")


    def to_json(self) -> str:
        result = {}
        for cat in ResultCategory:
            result[cat.name] = [ x.to_dict() for x in self._messages if x.category == cat ]
        return json.dumps(result, indent=2)


    def to_frame(self) -> pd.DataFrame:

        n_total = len(self._messages)
        category = np.zeros(n_total, dtype=object)
        location = np.zeros(n_total, dtype=object)
        message = np.zeros(n_total, dtype=object)
        time_ms = np.zeros(n_total, dtype=np.int)

        idx = 0
        for cat in ResultCategory:
            messages = self.by_category(cat)
            for x in messages:
                category[idx], location[idx], message[idx], time_ms[idx] = cat.value.upper(), x.location, x.message, x.ms
                idx += 1

        df = pd.DataFrame({
            "category": category, "location": location, "message": message, "ms": time_ms
        })
        return df

    def to_csv(self) -> str:
        df = self.to_frame()
        dest = io.StringIO()
        df.to_csv(dest, index=False)
        return dest.getvalue()

    def format_table(self, cat: ResultCategory) -> List[str]:

        lines = []
        lines.append("<table class='result-table'>")
        lines.append(f"  <caption>{cat.value.upper()}</caption>")

        lines.append(f"  <tr>")
        lines.append(f"    <th class='category'>Category</td>")
        lines.append(f"    <th class='location'>Location</td>")
        lines.append(f"    <th class='message'>Message</td>")
        #lines.append(f"   <th class='ms'>time (ms)</td>")
        lines.append(f"  </tr>")

        xcls = cat.value.lower().replace(" ", "-")

        messages = self.by_category(cat)
        for x in messages:
            lines.append(f"  <tr class='{xcls}'>")
            lines.append(f"    <td class='category'>{cat.value.upper()}</td>")
            lines.append(f"    <td class='location'>{x.location}</td>")
            msg = html.escape(x.message).replace('\n', '<br>\n')
            lines.append(f"    <td class='message'>{msg}</td>")
            #lines.append(f"   <td class='ms'>{x.ms}</td>")
            lines.append(f"  </tr>")

        lines.append("</table>")
        return lines

    def to_html(self, as_fragment=False) -> str:
        lines = []

        if not as_fragment:
            lines.append("<html>")
            lines.append("  <head>")
            lines.append("    <link rel='stylesheet' href='/static/result_log.css' type='text/css' />")
            lines.append("  </head>")
            lines.append("  <body>")

        lines.append("    <div class='data-container'>")
        for cat in ResultCategory:
            lines.append("    <div class='data-panel'>")

            new_lines = self.format_table(cat)
            lines.extend([ "      " + x for x in new_lines])
            lines.append("    </div>")
        lines.append("    </div>")

        if not as_fragment:
            lines.append("  </body>")
            lines.append("</html>")

        return "\n".join(lines)


# -----------------------------
def test():

    log = ResultLog()
    log.data_quality("NY", "Looking kinda scary.  > 50K")
    log.data_source("TX", "We're missing stuff, find it")
    log.operational("FL", '"Let''s Ignore It"')

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