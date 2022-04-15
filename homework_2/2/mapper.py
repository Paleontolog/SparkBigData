#!/usr/bin/python3
import sys
sys.path.append('.')

def start():
    population = {}

    for line in sys.stdin:
        line = line.strip()
        year, age, ethnic, sex, area, count = line.split(",")
        
        if "Year" in year:
          continue
          
        info = "{}-{}".format(year, area)
        try:
            count = int(count)
        except:
            count = 0

        if info in population:
            ethnic_count = population[info]
            if ethnic == "2":
                ethnic_count[0] += count
            ethnic_count[1] += count
        else:
            population[info] = [count, 0] if ethnic == "2" else [0, count]

    for key, value in population.items():
        print('{}, {}-{}'.format(key, value[0], value[1]))


start()


