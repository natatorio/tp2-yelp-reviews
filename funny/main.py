import sys
import os
sys.path.append(os.path.dirname(os.path.abspath('main.py')))
from consumers import *

def main():
    querier = CounterBy(keyId = 'city', exchange = 'reviews', routingKey = 'funny')
    funnyPerCity = querier.count()
    topTenFunnyPerCity = dict([(city, funnyPerCity[city]) for city in sorted(funnyPerCity, key=funnyPerCity.get, reverse=True)[:10]])

    print(len(funnyPerCity), " Funny Cities")
    querier.reply(topTenFunnyPerCity)
    querier.close()

if __name__ == '__main__':
    main()
