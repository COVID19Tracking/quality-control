# General Error Log
#    To handle if external sources fail
from loguru import logger

class ErrorLog:

    def __init__(self):
        self.has_error = False
        self.messages = []

    def error(self, msg: str):
        self.has_error = True
        logger.error(msg)
        self.messages.append(("ERROR", msg))

    def warning(self, msg: str):
        logger.warning(msg)
        self.messages.append(("WARNING", msg))

    def exception(self, ex: Exception):
        self.has_error = True
        logger.exception(ex)
        self.messages.append(("EXCEPTION", f"{ex}"))

    # ----

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

        for lev, msg in self.messages:
            lines.append(f"{lev},{escape(msg)}")

        if self.has_error:
            lines.append(f"ERROR,COULD NOT RUN")
        return "\n".join(lines)

    def to_json(self) -> str:
        return {
            "error": self.has_error,
            "message": [{ "level": lev, "message": msg } for lev, msg in self.messages] 
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
        for lev, msg in self.messages:            
            lines.append(f"    <div class='{lev.lower()}'>{lev}: {msg}</div>")
        lines.append("    </div>")

        if not as_fragment:
            lines.append("  </body>")
            lines.append("</html>")

        return "\n".join(lines)
