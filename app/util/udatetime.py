# date utilities
#
# rules:
#   1. always use dates with timezone=UTC
#   2. internal state uses UTC
#   3. end-user visible dates use ET
#

from datetime import datetime, timezone, timedelta
import pytz
import re
from typing import Tuple
import os
import pandas as pd

eastern_tz = pytz.timezone("US/Eastern")

def standardize_date(s: str) -> Tuple[str,int]:
    """reformat string into mm/dd/yyyy hh:mm format

    error_num is 0,1,2,3:
       0 = no error
       1 = changed
       2 = blank
       3 = missing date
       4 = missing time
       5 = bad date
       6 = bad time

    return standardize-date (as a string), error_num
    """

    error_num = 0

    # print(f"date in  >>{s}<<")
    if "-" in s:
        # yyyy-mm-dd mm:hh format
        idx = s.index(" ")
        if idx > 0:
            stime = s[idx:]
            sdate = s[0:idx]
        else:
            stime = "00:00"
            sdate = s
        parts = sdate.split("-")
        sdate = parts[1] + "/" + parts[2] + "/" + parts[0]
        error_num = 1 # changed
    elif "/" in s:
        if s[3] == '/' and s[5] == '/' and len(s) == 16:
            pass
        else:
            # m/d/y h:m format
            idx = s.find(" ")
            if idx > 0:
                stime = s[idx:]
                sdate = s[0:idx]
            else:
                stime = "00:00"
                sdate = s
            parts = sdate.split("/")
            if len(parts[0]) == 1: 
                parts[0] = "0" + parts[0]
            if len(parts[1]) == 1: 
                changed = True
                parts[1] = "0" + parts[1]
            if len(parts) == 2: 
                # allow missing year
                # changed = True
                parts.append("2020")
            sdate = parts[0] + "/" + parts[1] + "/" + parts[2]
    elif s == "":
        sdate = "01/01/2020"
        stime = "00:00"
        error_num = 2 # blank
    else:
        dt = now_as_eastern()
        sdate = f"{dt.month:02}/{dt.day:02}/{dt.year}"        
        stime = s
        error_num = 3 # missing date

    # convert to 24 hour clock
    has_pm = stime.endswith("PM")
    if has_pm or stime.endswith("AM"):
        if error_num == 0: error_num = 1 # changed
        stime = stime[0:-2].strip()
        parts = stime.split(":")
        h = int(parts[0])
        m = int(parts[1])
        if has_pm: h += 12
        if not (0 < h < 24):
            h = 23
            error_num = 6 # bad time
        if not (0 < m < 60):
            m = 0
            error_num = 6 # bad time
        stime = f"{h:02d}:{m:02d}"
    elif len(stime) == 0:
        error_num = 4 # missing time

    s =  sdate.strip() + " " + stime.strip()
    # print(f"date out >>{s}<< changed = {changed}")
    return s, error_num


#
# Do not use utcnow.  It returns a naive date at current time in England.
#

def now_as_utc() -> datetime:
    """ get current time as a tz-aware datetime
    if you are on CT and your local time is 4pm, the UTC time is 9pm
    """
    xnow = datetime.now().astimezone(pytz.UTC)
    return xnow

def now_as_eastern() -> datetime:
    """ get current time on east-coast as a tz-aware datetime
    """
    xnow = datetime.now().astimezone(eastern_tz)
    return xnow

def naivedatetime_as_eastern(dt: datetime) -> datetime:
    """ convert a python date into as a tz-aware datetime
    """
    if dt == None: return None
    if type(dt) != datetime:
        raise Exception(f"type ({type(dt)}) is not datetime")
    if dt.tzname() != None:
        raise Exception(f"value ({dt}) is not a naive datetime")
    return dt.astimezone(eastern_tz)

def pandas_timestamp_as_eastern(dt: pd.Timestamp) -> pd.Timestamp:
    """ convert a pandas date as a tz-aware datetime
    """
    if dt == None: return None
    if type(dt) != pd.Timestamp:
        raise Exception(f"type ({type(dt)}) is not pandas.TimeStamp")
    if dt.tzname() != None:
        raise Exception(f"value ({dt}) is not a naive timestamp")
    return dt.tz_localize(eastern_tz)


def now_as_local() -> datetime:
    """ get current time as a tz-aware datetime
    """
    xnow = datetime.now().astimezone()
    return xnow

def file_modified_at(xpath: str) -> datetime:
    """ get modification date of file """

    #print(xpath)
    mtime = os.path.getmtime(xpath)
    mtime = datetime.fromtimestamp(mtime).as_timezone().as_timezone(pytz.UTC)
    return mtime

def file_age(xpath: str) -> float:
    """ get age of a file in minutes """

    #print(xpath)
    mtime = os.path.getmtime(xpath)
    mtime = datetime.fromtimestamp(mtime)

    xnow = datetime.now()
    xdelta = (xnow - mtime).seconds / 60.0

    return xdelta

def to_filenameformat(dt: datetime) -> str:
    " format a date for use in file names "
    if dt == None: return None
    require_utc(dt)
    return dt.strftime('%Y%m%d-%H%M%SZ')

def to_logformat(dt: datetime) -> str:
    " format a date for use in logging messages "
    if dt == None: return "[none]"
    require_utc(dt)
    return dt.astimezone(eastern_tz).strftime('%Y-%m-%d %H:%M:%S %Z')

def to_displayformat(dt: datetime) -> str:
    " format a date to display to user "
    if dt == None: return ""
    require_timezone(dt)
    return dt.astimezone(eastern_tz).strftime('%Y-%m-%d %H:%M:%S %Z')

def from_json(s: str) -> datetime:
    " import a date from json, expected to be UTC "
    if type(s) != str: raise Exception(f"value ({s}) is a str")
    return datetime.fromisoformat(s).astimezone(pytz.UTC)

def to_json(dt: datetime) -> str:
    " export a date from json in isoformat "
    if dt == None: return None
    require_utc(dt)
    return dt.isoformat()

def from_local_naive(dt: datetime) -> datetime:
    " convert a  naive date in local time to tz-UTC"
    if dt.tzinfo != None:
        raise Exception(f"value ({dt} {dt.tzinfo}) already has tz")
    #logger.warning("converting local naive date to tz-UTC date")
    return dt.astimezone().astimezone(pytz.UTC)

def format_difference(dt1: datetime, dt2: datetime) -> str:
    " format the difference between two dates for display"
    if dt1 == None or dt2 == None: return ""
    require_utc(dt1)
    require_utc(dt2)

    if dt1 < dt2: return "NOW"
    delta = dt1 - dt2
    if delta.days != 0:
        return "OLD"
    else:
        sec = delta.seconds
        h = sec // (60*60)
        m = (sec - h * 60*60) // 60
        return f"{h:02d}:{m:02d}"

def is_isoformated(s: str) -> False:
    " test if value is an iso-formatted string"
    if type(s) != str: return False
    #2020-03-13T06:17:50.204477
    return re.match("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]{6})?", s)

def format_mins(x : float) -> str:
    if x < 60.0:
        return f"{x:.0f} mins"
    x /= 60.0
    if x < 24.0:
        return f"{x:.1f} hours"
    return f"{x:.1f} days"

def require_timezone(dt: datetime) -> datetime:
    " require value to be an tz-aware date "
    if dt == None: return None
    if type(dt) != datetime:
        if type(dt) == str:
            if is_isoformated(dt):
                raise Exception(f"value ({dt}) is a str containing an isoformated date")
        raise Exception(f"type ({type(dt)}) is not datetime")
    if dt.tzname() == None:
        raise Exception(f"value ({dt}) is a naive datetime")
    return dt

def require_utc(dt: datetime) -> datetime:
    " require value to be an tz-UTC date "
    if dt == None: return None
    if type(dt) != datetime:
        if type(dt) == str:
            if is_isoformated(dt):
                raise Exception(f"value ({dt}) is a str containing an isoformated date")
        raise Exception(f"type ({type(dt)}) is not datetime")
    if dt.tzname() != "UTC":
        raise Exception(f"value ({dt}, dt.tzname={dt.tzname()}) is not UTC")
    return dt
