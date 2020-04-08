# General Error Log
#    To handle if external sources fail
from loguru import logger
import html

class ErrorLog:

    def __init__(self):
        self.has_error = False
        self.messages = []

    def error(self, msg: str, exception: Exception = None):
        self.has_error = True
        if exception != None:
            logger.exception(exception)
        logger.error(msg)
        self.messages.append(("ERROR", msg, exception))

    def warning(self, msg: str, exception: Exception = None):
        logger.warning(msg)
        if exception != None:
            logger.exception(exception)
        self.messages.append(("WARNING", msg, exception))

    # ----
    def format_message(self, msg: str, ex: Exception):
        if ex == None: return msg
        return f"{msg}: {ex}"

    def print(self):
        if self.has_error:
            logger.error("Could not complete run")
        pass

    def to_csv(self) -> str:
        lines = ["LEVEL,MESSAGE"]

        def escape(m: str) -> str:
            if m == None: return ""
            if "," in m or '"' in m:
                m = m.replace('"', '""')
                m = '"' + m + '"'
            return m.replace("\n", ";")

        for lev, msg, ex in self.messages:
            m = self.format_message(msg, ex)
            lines.append(f"{lev},{escape(m)}")

        if self.has_error:
            lines.append(f"ERROR,COULD NOT RUN")
        return "\n".join(lines)

    def to_json(self) -> str:
        return {
            "error": self.has_error,
            "message": [{ "level": lev, "message": self.format_message(msg, ex) } for lev, msg, ex in self.messages]
        }


    def to_html(self, as_fragment=False) -> str:
        lines = []

        if not as_fragment:
            lines.append("<html>")
            lines.append("  <head>")
            lines.append("    <link rel='stylesheet' href='/static/result_log.css' type='text/css' />")
            lines.append("  </head>")
            lines.append("  <body>")

        lines.append("    <div class='error-container'>")
        for lev, msg, ex in self.messages:
            m = html.escape(self.format_message(msg, ex))
            lines.append(f"    <div class='{lev.lower()}'>{lev}: {m}</div>")
        lines.append("    </div>")

        if not as_fragment:
            lines.append("  </body>")
            lines.append("</html>")

        return "\n".join(lines)
