#!/usr/bin/python3
import sys
sys.path.append('.')

def start():
    population = {}

    for line in sys.stdin:
        line = line.strip()
        line = line.split(",")
        if len(line) == 6:
          year, age, ethnic, sex, area, count = line
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
        else:
          code, name, _ = line
          if "Code" in code:
            continue
          print('{},{}'.format(code, name))
        
    for key, value in population.items():
        year, area = key.split("-")
        print('{},{}^{}^{}'.format(area, year, value[0], value[1]))


start()


