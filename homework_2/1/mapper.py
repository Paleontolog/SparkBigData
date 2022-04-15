#!/usr/bin/python3
import sys
sys.path.append('.')


def get_info_map(name):
    result_map = {}
    with open(name, "r") as r:
        for line in r.readlines()[1:]:
            line = line.split(",")
            result_map[line[0]] = line[1]
    return result_map


def type_1():
    for line in sys.stdin:
        line = line.strip()
        year, age, ethnic, sex, area, count = line.split(",")
        
        if "Year" in year:
          continue
          
        try:
          count = int(count)
        except:
          count = 0
        print('{0}-{1},{2}-{3}'.format(year, area, ethnic, count))

get_info_map("DimenLookupArea8277")
 
type_1()

