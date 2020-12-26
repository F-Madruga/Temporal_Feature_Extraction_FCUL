import os
import sys
import random

averages = {}
stddev = 100
number_of_lines = 1000000
number_of_sellers = number_of_lines / 1000
hacks = 20
hack_odds = 3

def row(i):
    time = i
    
    t = time / 1000
    day = (t % 30) + 1
    month = ((t / 30) % 12) + 1
    year = (t / 30 / 12) + 2016
    amount = random.randint(10, 10000)
    buyer = random.randint(0, 50)
    seller = random.randint(0, number_of_sellers)
    credit_card = random.randint(0, 100000)
    latitude = random.randint(-500, 500)
    longitude = random.randint(-500, 500)
    return [time, year, month, day, amount, buyer, seller, credit_card, latitude, longitude]

def row_as_str(r):
    return ",".join([ str(e) for e in r ]) + "\n"

if __name__ == '__main__':
    with open("dataset.csv", "w") as f:
        for i in range(number_of_lines):
            f.write(row_as_str(row(i)))