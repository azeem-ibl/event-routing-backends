"""
Support for reading tracking event logs.

Taken entirely from edx-analytics-pipeline.
"""
import json
import logging
import re
from json.decoder import JSONDecodeError

log = logging.getLogger(__name__)

def get_event_string_from_line(string, raise_error=True):
    """
    For edX tutor instance, we have some events that have been emitted prior to the ibl_edx_tutor_logger configuration
    Hence we need to extract json event string from line by excluding the logger variables
    e.g 2022-04-26 16:49:40,618 INFO 29 [tracking] [user None] [ip 172.18.0.1] logger.py:42 - {"name": "/register", "context": {"user_id": null, "path": "/register", "course_id": "", "org_id": ""}, "username": "", "session": "", "ip": "11.10.135.72", "agent": "Mozilla/5.0 (Linux; Android 7.0;) AppleWebKit/537.36 (KHTML, like Gecko) Mobile Safari/537.36 (compatible; PetalBot;+https://webmaster.petalsearch.com/site/petalbot)", "host": "198.50.158.98", "referer": "", "accept_language": "en", "event": "{\"GET\": {\"next\": [\"/blog\"]}, \"POST\": {}}", "time": "2022-04-26T16:49:40.618241+00:00", "event_type": "/register", "event_source": "server", "page": null}
    """
    try:
        json.loads(string)
        return string
    except ValueError as e: # previous tutor based edX logs
        try:
            return re.search("- (.*)", string, re.DOTALL).group(1)
        except AttributeError:
            if not raise_error:
                return string
            raise ValueError(e)


def parse_json_event(line):
    """
    Parse a tracking log input line as JSON to create a dict representation.

    Arguments:
    * line:  the eventlog text
    """
    try:
        parsed = get_event_string_from_line(line)

        # The representation of an event that event-routing-backends receives
        # from the async sender if significantly different from the one that
        # are saved to tracking log files for reasons lost to history.
        # This section of code attempts to format the event line to match the
        # async version.

        try:
            # The async version uses "data" for what the log file calls "event".
            # Sometimes "event" is a nested string of JSON that needs to be parsed.
            parsed["data"] = json.loads(parsed["event"])
        except (TypeError, JSONDecodeError):
            # If it's a TypeError then the "event" was not a string to be parsed,
            # so probably already a dict. If it's a JSONDecodeError that means the
            # "event" was a string, but not JSON. Either way we just pass the value
            # back, since all of those are valid.
            parsed["data"] = parsed["event"]

        # The async version of tracking logs seems to use "timestamp" for this key,
        # while the log file uses "time". We normalize it here.
        if "timestamp" not in parsed and "time" in parsed:
            parsed["timestamp"] = parsed["time"]

        return parsed
    except (AttributeError, JSONDecodeError, KeyError) as e:
        log.error("EXCEPTION!!!")
        log.error(type(e))
        log.error(e)
        log.error(line)

        return None
