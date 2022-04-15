#!/usr/bin/python3
import sys
sys.path.append('.')


def get_info_map(name):
    result_map = {}
    with open(name, "r") as r:
        for line in r.readlines()[1:]:
            line = line.strip()
            line = line.split(",")
            result_map[line[0]] = line[1]
    return result_map

def compute_result(population, areas_map):
    
    for key, value in population.items():
        year, area = key.split("-")
        if value[1] != 0:
            print('{},{},{},{},{},{}'.format(year, areas_map.get(area, "error"), area, value[0], value[1], value[0] / value[1]))
        else:
            print('{},{},{},{},{},{}'.format(year, areas_map.get(area, "error"), area, value[0], value[1], 0))


def type_1():
    population = {}
    areas_map = get_info_map("DimenLookupArea8277")

    for line in sys.stdin:
        line = line.strip()
        info, data = line.split(',')
        ethnic, count = data.split("-")

        count = int(count)

        if info in population:
            ethnic_count = population[info]
            if ethnic == "2":
                ethnic_count[0] += count
            ethnic_count[1] += count
        else:
            population[info] = [count, 0] if ethnic == "2" else [0, count]

    compute_result(population, areas_map)


type_1()
