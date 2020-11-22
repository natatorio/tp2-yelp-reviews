import sys
import os
sys.path.append(os.path.dirname(os.path.abspath('main.py')))
from consumers import *

def main():
    counter = CounterByWeekday(keyIds = ['weekday'], exchange = 'reviews', routingKey = 'histogram')
    histogram = counter.count()

    counter.reply(histogram)
    counter.close()

if __name__ == '__main__':
    main()
