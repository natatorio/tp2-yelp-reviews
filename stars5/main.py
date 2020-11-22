import sys
import os
sys.path.append(os.path.dirname(os.path.abspath('main.py')))
from consumers import *

def main():
    querier = Stars5Querier(keyIds = ['user_id'], exchange = 'reviews', routingKey = 'stars5')
    stars5ReviewsPerUser = querier.count()
    allStars5ReviewsPerRelevantUser = querier.join(stars5ReviewsPerUser)

    print(len(stars5ReviewsPerUser), " Users reviewing with 5 star")
    print(len(allStars5ReviewsPerRelevantUser), " Relevant Users reviewing all with 5 star")
    querier.reply(allStars5ReviewsPerRelevantUser)
    querier.close()

if __name__ == '__main__':
    main()
