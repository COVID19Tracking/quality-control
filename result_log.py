# ResultLog -- collects results for a run and handle printing/posting them
#
# Each message is assigned a category
#
from enum import Enum
import json
import io
import pandas as pd
import numpy as np
import udatetime
from typing import Tuple, Dict, List
import time
import html

class ResultCategory(Enum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"

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
        return { "category": self.category.name, "location": self.location, "message": self.message, "ms": self.ms }

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

    def error(self, location: str, message: str) -> None:
        self.add(ResultCategory.ERROR, location, message)
    def warning(self, location: str, message: str) -> None:
        self.add(ResultCategory.WARNING, location, message)
    def info(self, location: str, message: str) -> None:
        self.add(ResultCategory.INFO, location, message)

    # -----

    def print(self):

        print("")

        if len(self._messages) == 0:
            print("[No Messages]")

        for cat in ResultCategory:
            messages = self.by_category(cat)
            if len(messages) == 0: continue
        
            print(f"=====| {cat.name} |===========")
            for x in messages:
                print(f"{x.location}: {x.message}")

        print("")


    def to_json(self) -> str:
        result = {
            "error": [ x.to_dict() for x in self._messages if x.category == ResultCategory.ERROR ],
            "warning": [ x.to_dict() for x in self._messages if x.category == ResultCategory.WARNING ],
            "infomation": [ x.to_dict() for x in self._messages if x.category == ResultCategory.INFO ],
        }
        return json.dumps(result, indent=2)


    def to_frame(self) -> pd.DataFrame:
 
        n_total = len(self._messages)
        level = np.zeros(n_total, dtype=object)
        location = np.zeros(n_total, dtype=object)
        message = np.zeros(n_total, dtype=object)
        time_ms = np.zeros(n_total, dtype=np.int)

        idx = 0
        for cat in ResultCategory:
            messages = self.by_category(cat)
            for x in messages:
                level[idx], location[idx], message[idx], time_ms[idx] = cat.name, x.location, x.message, x.ms
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
    
    def to_html(self, as_fragment=False) -> str:
        lines = []

        if not as_fragment:
            lines.append("<html>")
            lines.append("  <head>")
            lines.append("    <link rel='stylesheet' href='result_log.css' type='text/css' />")
            lines.append("  </head>")
            lines.append("  <body>")

        lines.append("    <table class='result-table'>")

        lines.append(f"      <tr>")
        lines.append(f"        <th class='category'>Category</td>")
        lines.append(f"        <th class='location'>Location</td>")
        lines.append(f"        <th class='message'>Message</td>")
        #lines.append(f"        <th class='ms'>time (ms)</td>")
        lines.append(f"      </tr>")

        for cat in ResultCategory:
            messages = self.by_category(cat)
            for x in messages:
                lines.append(f"      <tr class='{cat.name}'>")
                lines.append(f"        <td class='category'>{x.category}</td>")
                lines.append(f"        <td class='location'>{x.location}</td>")
                msg = html.escape(x.message).replace('\n', '<br>\n')
                lines.append(f"        <td class='message'>{msg}</td>")
                #lines.append(f"        <td class='ms'>{x.ms}</td>")
                lines.append(f"      </tr>")

        lines.append("    </table>")

        if not as_fragment:
            lines.append("  </body>")
            lines.append("</html>")

        return "\n".join(lines)


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
