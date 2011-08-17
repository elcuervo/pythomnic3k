#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
###############################################################################
#
# This module implements the performance graph displaying HTTP interface.
# If the "performance" interface has been started in config_interfaces.py,
# this module is called to process HTTP request incoming through it.
# Returned to each request is an interactive clickable text-only
# HTML page with performance diagrams.
#
# The working of this module is intimately tied with the internals of
# performance.py, for example it is presumed that there are exactly two
# kinds of performance readings - rates and times, and they have particular
# names, such as resource.foo.transaction_rate. The ranges (last hour plus
# one last minute) and scaling (0-39) of the collected data are also fixed
# to match that of the performance.py's (see extract() method).
#
# Pythomnic3k project
# (c) 2005-2010, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
################################################################################

__all__ = [ "process_request" ]

################################################################################

import io; from io import StringIO
import datetime; from datetime import datetime, timedelta
import urllib.parse; from urllib.parse import urlparse

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import typecheck; from typecheck import typecheck, optional, by_regex, with_attr, \
                                        dict_of, nothing

###############################################################################
# unicode box characters

b1 = "&#x2581;"; b2 = "&#x2582;"; b3 = "&#x2583;"; b4 = "&#x2584;"
b5 = "&#x2585;"; b6 = "&#x2586;"; b7 = "&#x2587;"; b8 = full_block = "&#x2588;"

box_chars = (b1, b2, b3, b4, b5, b6, b7, b8)

###############################################################################
# this method takes low-high value in range 0-39 and returns a single
# representing bar character

@typecheck
def _collapsed_bar(s: optional((int, int))) -> str:

    if s is None:
        return " "

    low, high = s; assert 0 <= low <= high <= 39
    return box_chars[high // 5]

###############################################################################
# this method takes low-high value in range 0-39 and returns a column of 5
# representing characters, the ones prefixed with ~ should appear in negative

@typecheck
def _expanded_bar(s: optional((int, int))) -> [str, str, str, str, str]:

    if s is None:
        return [" "] * 5

    low, high = s; assert 0 <= low <= high <= 39

    low_idx, low_level = divmod(low, 8)
    high_idx, high_level = divmod(high, 8)

    result = [" "] * low_idx
    if low_idx < high_idx:
        result.append(low_level > 0 and ("~" + box_chars[low_level - 1]) or full_block)
        result.extend([full_block] * (high_idx - low_idx - 1))

    result.append(box_chars[high_level])
    result.extend([" "] * (4 - high_idx))

    return result

###############################################################################
# this method replaces certain formatting characters with fancy box-drawing ones

def _decorate(line: str) -> str:
    result = ""
    pc = None
    for c in line:
        if c == " ":
            if pc != " ": result += "<span class=\"bk\">"
            result += full_block
        else:
            if pc == " ": result += "</span>"
            if c == "-": result += "&#x2014;"
            elif c == "+": result += "&#x00b7;"
            elif c == "_": result += " "
            elif c == "~": result += "&#x2015;"
            elif c == "*": result += "+"
            else: result += c
        pc = c
    if pc == " ": result += "</span>"
    return result

###############################################################################

def _quote(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").\
             replace("\"", "&quot;").replace("'", "&apos;")

###############################################################################

_hrule = "---------+---------+---------+---------+---------+---------+---------+"
_ref_times = list(map(_decorate, ["10.0~", " 3.0~", " 1.0~", " 0.3~", " 0.1~"]))
_ref_rates = list(map(_decorate, [" 100~", "  30~", "  10~", "   3~", "   1~"]))

legends = { "request_rate": "request rate",
            "response_rate": "response rate",
            "response_rate.success": "successful response rate",
            "response_rate.failure": "failed responses rate",
            "transaction_rate": "transaction rate",
            "transaction_rate.success": "successful transaction rate",
            "transaction_rate.failure": "failed transaction rate",
            "pending_time": "pending time",
            "processing_time": "processing time",
            "processing_time.success": "successful processing time",
            "processing_time.failure": "failed processing time",
            "response_time": "response time",
            "response_time.success": "successful response time",
            "response_time.failure": "failed response time" }

default_reading_modes = { "interface": ("response_time", "collapsed"),
                          "resource": ("processing_time" , "collapsed") }

optional_readings = { "interface": ("request_rate", "pending_time", "processing_time", "response_time"),
                      "resource": ("transaction_rate", "pending_time", "processing_time") }

css_style = """\
<style type=\"text/css\"><!--
.default {{ color: black; background-color: white; vertical-align: bottom; font-family: {css_font_family:s}; }}
.bk {{ color: white }}
.c4 {{ color: green }}
.c4i {{ color: white; background-color: green }}
.c3 {{ color: forestgreen }}
.c3i {{ color: white; background-color: forestgreen }}
.c2 {{ color: yellowgreen }}
.c2i {{ color: white; background-color: yellowgreen }}
.c1 {{ color: goldenrod }}
.c1i {{ color: white; background-color: goldenrod }}
.c0 {{ color: orangered }}
.c0i {{ color: white; background-color: orangered }}
a {{ color: darkblue; text-decoration: none }}
//--></style>"""

###############################################################################
# this method takes a dict containing parsed URL query and one of the optional
# mangling arguments, assembles and returns string with modified URL query

def _format_modified_query(query: dict, *, expand = None, collapse = None, replace = None):

    d = query.copy()

    if expand:
        d[expand] = "expanded"
    if collapse:
        d[collapse] = "collapsed"
    if replace:
        k, reading = replace
        del d[k]
        k = ".".join(k.split(".", 2)[:2]) + "." + reading
        d[k] = "expanded"

    return "&".join("{0:s}={1:s}".format(k, v) for k, v in d.items())

###############################################################################

@typecheck
def _performance_report(html: with_attr("write"), query: dict_of(str, str),
                        request: dict, response: dict) -> nothing:

    # fetch the performance dump, this can return None

    stats = pmnc.performance.extract()
    if stats is None:
        html.write("<html><body>cage {0:s} has nothing to report yet"
                   "</body></html>".format(__cage__))
        return

    # extract main thread pool stats

    req_active, req_pending, req_rate = pmnc.interfaces.get_activity_stats()

    # extract global transaction rate

    xa = pmnc.transaction.create(); xa.execute()
    txn_rate = xa.get_transaction_rate()

    base_time, stats_dump, app_perf = stats
    base_dt = datetime.fromtimestamp(base_time)

    # see data for what objects is available, will have to display at least one graph for each

    monitored_objects = set(tuple(k.split(".", 2)[:2]) for k in stats_dump.keys())
    requested_objects = set(tuple(k.split(".", 2)[:2]) for k in query.keys())

    displayed_objects = {}
    for object_type, object_name in monitored_objects - requested_objects:
        reading, mode = default_reading_modes[object_type]
        displayed_objects["{0:s}.{1:s}.{2:s}".format(object_type, object_name, reading)] = mode

    # add/override explicitly requested graphs from the URL query

    for k, v in query.items():
        if k in stats_dump:
            displayed_objects[k] = v

    # reassemble the canonical URL query

    canonical_query = _format_modified_query(displayed_objects)

    # format horizontal time scales

    base_minute = base_dt.minute % 10
    hrule = "---- "  + _hrule[base_minute:][:59] + "   -----"
    plus_count = hrule.count("+")

    base_dt10m = datetime.fromtimestamp(base_time // 600  * 600)
    base_dt10m -= timedelta(minutes = (plus_count - (base_minute != 0 and 1 or 0)) * 10)
    hhmms = [ (base_dt10m + timedelta(minutes = i * 10)).strftime("%H:%M")
              for i in range(plus_count) ]

    hscale = " " * (hrule.index("+") - 2) + "     ".join(hhmms)
    hscale += " " * (len(hrule) - len(hscale) - 5) + base_dt.strftime("%H:%M")

    hrule = _decorate(hrule)
    hscale = _decorate(hscale)

    # format header

    cpu_ut_percent = int(app_perf.get("cpu_ut_percent", 0.0))
    cpu_kt_percent = int(app_perf.get("cpu_kt_percent", 0.0))

    if cpu_kt_percent > 0:
        cpu_percent = "{0:d}*{1:d}".format(cpu_ut_percent, cpu_kt_percent)
    else:
        cpu_percent = "{0:d}".format(cpu_ut_percent)

    cage_at_node = "cage {0:s} at node {1:s}".format(__cage__, __node__)
    activity_info = "{0:d}{1:s} req, {2:.01f} req/s, {3:.01f} txn/s, {4:d} M RAM, {5:s} % CPU".\
                    format(req_active, req_pending and "*{0:d}".format(req_pending) or "",
                           req_rate, txn_rate, app_perf.get("wss", 0), cpu_percent)

    # write page header

    html.write("<html>"
               "<head>" +
               css_style.format(css_font_family = pmnc.config.get("css_font_family")) +
               "<title>Performance report for " + cage_at_node + "</title>"
               "</head>"
               "<body class=\"default\">"
               "<a href=\"/notifications\">logs</a>" + _decorate("  {0:s}<br/>".format(cage_at_node.center(58))) +
               _decorate("      {0:s}  {1:s}<br/><br/>\n".format(activity_info.center(58), base_dt.strftime("%b %d"))))

    html.write("<span style=\"line-height: 1.0;\">\n" + hscale + "<br/>\n" + hrule + "<br/>\n")

    # loop through the statistics items to display

    for k, mode in sorted(displayed_objects.items()):

        if k not in stats_dump: # invalid interface/resource name in URL ?
            continue

        s1m, s10s = stats_dump[k]
        s10s = s10s[:5]
        object_type, object_name, reading = k.split(".", 2)

        if mode == "collapsed": # draw collapsed graph

            def append_bars(b):
                result = ""
                for i, v in enumerate(b):
                    c = _collapsed_bar(v)
                    high = v is not None and v[1] or 0
                    result += "<span_class=\"c{0:d}\">{1:s}</span>".format(4 - high // 8, c)
                return result

            line = "<nobr>     {0:s}   {1:s}  ".format(append_bars(s1m), append_bars(s10s))
            html.write(_decorate(line))

            # append the clickable expand link

            expand_query = _format_modified_query(displayed_objects, expand = k)
            html.write("<a href=\"/performance?{0:s}\">{1:s} {2:s} ({3:s})</a></nobr><br/>".\
                       format(expand_query, object_type, object_name, legends[reading]))

        elif mode == "expanded": # draw expanded graph

            bars1m = list(zip(*map(_expanded_bar, s1m)))
            bars10s = list(zip(*map(_expanded_bar, s10s)))

            def append_bars(b, i):
                result = ""
                b = b[4 - i]
                for j in range(len(b)):
                    c = b[j]
                    if c.startswith("~"):
                        result += "<span_class=\"c{0:d}i\">{1:s}</span>".format(i, c[1:]) # note the underscore, will be converted to space
                    else:
                        result += c
                return result

            # expanded form has vertical scale, different for rates and times

            if "_time" in k:
                ref = _ref_times
            elif "_rate" in k:
                ref = _ref_rates

            # draw each of the 5 horizontal bars

            opt_readings = optional_readings[object_type]

            for i in range(5):

                line = "<nobr>" + ref[i] + "<span class=\"c{0:d}\">".format(i)
                _line = append_bars(bars1m, i)
                _line += "</span> ~ <span_class=\"c{0:d}\">".format(i) + append_bars(bars10s, i) + "  "
                line += _decorate(_line) + "</span>"

                if i == 0: # append the clickable collapse link
                    collapse_query = _format_modified_query(displayed_objects, collapse = k)
                    line += "<a href=\"/performance?{0:s}\">{1:s} {2:s}</a>".\
                            format(collapse_query, object_type, object_name)
                elif i <= len(opt_readings): # append the clickable selector links
                    opt_reading = opt_readings[i - 1]
                    modify_query = _format_modified_query(displayed_objects, replace = (k, opt_reading))
                    line += "{0:s}<a href=\"/performance?{1:s}\">{2:s}</a>{3:s}".\
                            format(reading == opt_reading and _decorate("&raquo; ") or _decorate("  "),
                                   modify_query, legends[opt_reading],
                                   reading == opt_reading and _decorate(" &laquo;") or _decorate("  "))

                html.write(line + "</nobr><br/>\n")

        html.write(hrule + "<br/>\n")

    # complete the response

    html.write(hscale + "<br/>\n</span>\n</body></html>")

    # require a refresh within a configured time

    refresh_seconds = pmnc.config.get("refresh_seconds")
    response["headers"]["refresh"] = \
        "{0:d};URL=/performance?{1:s}".format(refresh_seconds, canonical_query)

###############################################################################

@typecheck
def _notifications_report(html: with_attr("write"), query: dict_of(str, str),
                          request: dict, response: dict) -> nothing:

    # reassemble the canonical URL query

    canonical_query = "&".join("{0:s}={1:s}".format(k, v) for k, v in query.items())

    # format header

    cage_at_node = "cage {0:s} at node {1:s}".format(__cage__, __node__)

    # write page header

    html.write("<html>"
               "<head>" +
               css_style.format(css_font_family = pmnc.config.get("css_font_family")) +
               "<title>Notifications report for " + cage_at_node + "</title>"
               "</head>"
               "<body class=\"default\">"
               "<a href=\"/performance\">perf</a>" + _decorate("  {0:s}<br/>".format(cage_at_node.center(58))) +
               _decorate("most recent health monitor notifications".center(69)) + "<br/><br/>")

    # extract stored notifications and loop through it

    last_day = None

    for notification in reversed(pmnc.notify.extract()):

        message = _quote(notification["message"])

        timestamp = notification["timestamp"]
        day = datetime.fromtimestamp(timestamp).strftime("%b %d")
        hms = datetime.fromtimestamp(timestamp).strftime("%H:%M:%S")

        level = notification["level"]
        if level == "INFO":
            level = "<span class=\"c4\">INFO&nbsp;</span>"
        elif level == "WARNING":
            level = "<span class=\"c1\">WARN&nbsp;</span>"
        elif level == "ERROR":
            level = "<span class=\"c0\">ERROR</span>"
        elif level == "ALERT":
            level = "<strong><span class=\"c0\">ALERT</span></strong>"

        if day != last_day:
            html.write(_decorate(" {0:s} ----------------------------------------------------------------<br/>\n".format(day)))
            last_day = day

        html.write("{0:s}&nbsp;&nbsp;{1:s}&nbsp;&nbsp;<nobr>{2:s}</nobr><br/>\n".\
                   format(hms, level, message))

    # complete the response

    html.write(_decorate("------------------------------------------------------------------------<br/>\n"))
    html.write("</body></html>")

    # require a refresh within a configured time

    refresh_seconds = pmnc.config.get("refresh_seconds")
    response["headers"]["refresh"] = \
        "{0:d};URL=/notifications?{1:s}".format(refresh_seconds, canonical_query)

###############################################################################

valid_perf_query_element = "(interface|resource)\\.[A-Za-z0-9_-]+\\.({0:s})=(collapsed|expanded)".\
                           format("|".join(legends.keys()))
valid_perf_query = by_regex("^({0:s}(&{0:s})*)?$".format(valid_perf_query_element))
valid_ntfy_query = by_regex("^$")

###############################################################################
# this method is called from the HTTP interface for actual request processing

def process_request(request: dict, response: dict):

    parsed_url = urlparse(request["url"])
    path = parsed_url.path
    query = parsed_url.query
    html = StringIO()

    if path in ("/", "/performance"):

        if not valid_perf_query(query):
            raise Exception("invalid query format")
        query = query and dict(p.split("=") for p in query.split("&")) or {}

        _performance_report(html, query, request, response)

    elif path == "/notifications":

        if not valid_ntfy_query(query):
            raise Exception("invalid query format")
        query = query and dict(p.split("=") for p in query.split("&")) or {}

        _notifications_report(html, query, request, response)

    else:
        response["status_code"] = 404
        return

    response["content"] = html.getvalue()
    response["headers"]["content-type"] = "text/html"

###############################################################################

def self_test():

    from time import sleep
    from pmnc.request import fake_request

    ###################################

    def test_collapsed_bar():

        assert _collapsed_bar(None) == " "

        for low in range(40):
            for high in range(low + 1, 40):
                assert _collapsed_bar((low, high)) == box_chars[high // 5]

    test_collapsed_bar()

    ###################################

    def test_expanded_bar():

        assert _expanded_bar(None) == [" ", " ", " ", " ", " "]

        assert _expanded_bar((0, 0)) == [b1, " ", " ", " ", " "]
        assert _expanded_bar((0, 1)) == [b2, " ", " ", " ", " "]
        assert _expanded_bar((0, 4)) == [b5, " ", " ", " ", " "]
        assert _expanded_bar((0, 7)) == [b8, " ", " ", " ", " "]
        assert _expanded_bar((0, 8)) == [b8, b1, " ", " ", " "]
        assert _expanded_bar((0, 9)) == [b8, b2, " ", " ", " "]

        assert _expanded_bar((0, 39)) == [b8, b8, b8, b8, b8]
        assert _expanded_bar((1, 38)) == ["~" + b1, b8, b8, b8, b7]
        assert _expanded_bar((4, 35)) == ["~" + b4, b8, b8, b8, b4]
        assert _expanded_bar((7, 32)) == ["~" + b7, b8, b8, b8, b1]

        assert _expanded_bar((8, 31)) == [" ", b8, b8, b8, " "]
        assert _expanded_bar((9, 30)) == [" ", "~" + b1, b8, b7, " "]
        assert _expanded_bar((12, 27)) == [" ", "~" + b4, b8, b4, " "]
        assert _expanded_bar((15, 24)) == [" ", "~" + b7, b8, b1, " "]

        assert _expanded_bar((16, 23)) == [" ", " ", b8, " ", " "]
        assert _expanded_bar((17, 22)) == [" ", " ", b7, " ", " "]
        assert _expanded_bar((20, 20)) == [" ", " ", b5, " ", " "]

        assert _expanded_bar((0, 15)) == [b8, b8, " ", " ", " "]
        assert _expanded_bar((12, 27)) == [" ", "~" + b4, b8, b4, " "]
        assert _expanded_bar((24, 39)) == [" ", " ", " ", b8, b8]

        assert _expanded_bar((32, 39)) == [" ", " ", " ", " ", b8]
        assert _expanded_bar((35, 39)) == [" ", " ", " ", " ", b8]
        assert _expanded_bar((39, 39)) == [" ", " ", " ", " ", b8]

    test_expanded_bar()

    ###################################

    def test_notifications():

        request = dict(url = "/notifications",
                       method = "GET", headers = {}, body = b"")
        response = dict(status_code = 200, headers = {}, body = b"")

        pmnc.__getattr__(__name__).process_request(request, response)
        assert response["status_code"] == 200
        content = response["content"]

        assert "Notifications report" in content

    test_notifications()

    ###################################

    def test_performance():

        fake_request(120.0)

        for i in range(90):
            pmnc.performance.event("interface.foo.response_rate.success")
            pmnc.performance.sample("interface.foo.response_time.success", 10)
            pmnc.performance.event("resource.bar.transaction_rate.success")
            pmnc.performance.sample("resource.bar.processing_time.success", 10)
            sleep(1.0)
            pmnc.log("wait {0:d}/90".format(i + 1))

        request = dict(url = "/?"
                             "interface.foo.request_rate=expanded&"
                             "interface.foo.response_rate.success=collapsed&"
                             "interface.foo.response_time=expanded&"
                             "interface.foo.response_time.success=collapsed&"
                             "resource.bar.transaction_rate=expanded&"
                             "resource.bar.transaction_rate.success=collapsed&"
                             "resource.bar.processing_time=expanded&"
                             "resource.bar.processing_time.success=collapsed",
                       method = "GET", headers = {}, body = b"")
        response = dict(status_code = 200, headers = {}, body = b"")

        pmnc.__getattr__(__name__).process_request(request, response)
        assert response["status_code"] == 200
        content = response["content"]

        # difficult to assert anything reasonable about the response content here

        assert "10.0" in content and "100" in content
        assert "interface foo (successful response rate)" in content
        assert "interface foo (successful response time)" in content
        assert "resource bar (successful transaction rate)" in content
        assert "resource bar (successful processing time)" in content
        assert "request rate</a>" in content
        assert "response time</a>" in content
        assert "transaction rate</a>" in content
        assert "processing time</a>" in content
        assert "RAM" in content
        assert "CPU" in content

    test_performance()

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF
