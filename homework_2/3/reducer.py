#!/usr/bin/python3
import sys
from itertools import groupby
from operator import itemgetter

sys.path.append('.')

def read_mapper_output(file, separator=','):
    for line in file:
        yield line.strip().split(separator, 1)


def compute_result(population, areas_map):
    for key, value in population.items():
        year, area = key.split("-")
        if value[1] != 0:
            print('{},{},{},{},{},{}'.format(year, areas_map.get(area, "error"), area, value[0], value[1],
                                             value[0] / value[1]))
        else:
            print('{},{},{},{},{},{}'.format(year, areas_map.get(area, "error"), area, value[0], value[1], 0))

def type_1():
    population = {}
    areas_map = {}

    data = read_mapper_output(sys.stdin)

    for area_code, group in groupby(data, itemgetter(0)):

        group = [i[1] for i in group]

        area_name = list(filter(lambda x: "^" not in x, group))[0]
        data = list(filter(lambda x: "^" in x, group))[0]

        areas_map[area_code] = area_name

        data = data.split("^")

        data_year = data[0]
        data_year = data_year.strip()

        data_count = [int(i) for i in data[1:]]

        info = "{}-{}".format(data_year, area_code)

        if info in population:
            ethnic_count = population[info]
            ethnic_count[0] += data_count[0]
            ethnic_count[1] += data_count[1]
        else:
            population[info] = data_count

    compute_result(population, areas_map)


type_1()
