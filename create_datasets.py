import numpy as np
import pandas as pd
import random

class Point:
    def __init__(self, id, x, y):
        self.id = id
        self.x = x
        self.y = y

def generate_people(num):
    points = []
    for i in range(num):
        x = random.randrange(10000)
        y = random.randrange(10000)
        point = Point(i, x, y)
        points.append(point.__dict__)
    df = pd.DataFrame(points)
    return df

def generate_infected(people_df, num):
    df = people_df.sample(n=num)
    return df

num_people = 1000000
# generate one million people
people_df = generate_people(num_people)
# 50 people
infected_small_df = generate_infected(people_df, 50)
# 5% of total people
infected_large_df = generate_infected(people_df, int(num_people*.05))

people_df.to_csv('PEOPLE.csv', header=False, index=False)
infected_small_df.to_csv('INFECTED-small.csv', header=False, index=False)
infected_large_df.to_csv('INFECTED-large.csv', header=False, index=False)