import sys
import os
sys.path.append(os.path.dirname(os.path.abspath('main.py')))
from consumers import *

def main():
    bc = BusinessConsumer(exchange = 'reviews', routingKey = 'business')
    businessCities = bc.get_business_cities()
    bc.forward('reviews', 'funny', businessCities)

    print(len(businessCities), " Business Processed")
    bc.close()

if __name__ == '__main__':
    main()
