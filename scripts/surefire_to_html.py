#!/usr/bin/env python3
######################################################################
# A simple script to convert a surefire report from XML to HTML.
# see: https://www.erlang.org/doc/apps/eunit/eunit_surefire.html
# see: https://maven.apache.org/surefire/maven-surefire-report-plugin/
######################################################################
import xml
import sys
import xml.etree.ElementTree as ET

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
    element = ET.fromstringlist(data)
    return convert(element)

def from_file(file):
    tree = ET.parse(file)
    element = tree.getroot()
    return convert(element)

def convert(element):
    attrib = element.attrib
    print(f"""<table>
<tr><td>name</td><td>{attrib["name"]}</td></tr>
<tr><td>tests</td><td>{attrib["tests"]}</td></tr>
<tr><td>failures</td><td>{attrib["failures"]}</td></tr>
<tr><td>errors</td><td>{attrib["errors"]}</td></tr>
<tr><td>skipped</td><td>{attrib["skipped"]}</td></tr>
<tr><td>time</td><td>{attrib["time"]}</td></tr>
</table>""")
    return 0

if __name__ == '__main__':
    return main()
