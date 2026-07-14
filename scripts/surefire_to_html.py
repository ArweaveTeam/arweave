#!/usr/bin/env python3
######################################################################
# A simple script to convert a surefire report from XML to HTML.
#
# Handles both report shapes we produce:
#   - eunit_surefire -> the root is a single <testsuite>
#   - cth_surefire   -> the root is a <testsuites> wrapper holding one
#                       <testsuite> per Common Test suite
#
# see: https://www.erlang.org/doc/apps/eunit/eunit_surefire.html
# see: https://maven.apache.org/surefire/maven-surefire-report-plugin/
######################################################################
import sys
import xml.etree.ElementTree as ET

COLUMNS = ["name", "tests", "failures", "errors", "skipped", "time"]

def usage():
    print("Usage: %s [PATH|-]" % sys.argv[0])

def main():
    if len(sys.argv) <= 1:
        usage()
        return 1
    if sys.argv[1] == "-":
        return from_stdin()
    if sys.argv[1]:
        return from_file(sys.argv[1])
    return 1

def from_stdin():
    data = sys.stdin.readlines()
    return convert(ET.fromstringlist(data))

def from_file(file):
    return convert(ET.parse(file).getroot())

def testsuites(element):
    """The <testsuite> elements, whichever root shape we were handed."""
    if element.tag == "testsuites":
        return element.findall("testsuite")
    return [element]

def convert(element):
    suites = testsuites(element)
    if not suites:
        return 0
    header = "".join(f"<th>{column}</th>" for column in COLUMNS)
    rows = [
        "<tr>%s</tr>" % "".join(
            f"<td>{suite.attrib.get(column, '')}</td>" for column in COLUMNS
        )
        for suite in suites
    ]
    print("<table>\n<tr>%s</tr>\n%s\n</table>" % (header, "\n".join(rows)))
    return 0

if __name__ == '__main__':
    sys.exit(main())
