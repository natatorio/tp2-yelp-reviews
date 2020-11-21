import sys
import os
sys.path.append(os.path.dirname(os.path.abspath('main.py')))
from counter_by import *

def main():
    counter = UserCounterBy(keyIds = ['user_id'], exchange = 'reviews', routingKey = 'users')
    user_count = counter.count()
    user_count_5 = dict([u for u in user_count.items() if u[1] >= 5])
    user_count_50 = dict([u for u in user_count_5.items() if u[1] >= 50])
    counter.foward('reviews', 'stars5', user_count_50)
    counter.foward('reviews', 'comment', user_count_5)

    print(len(user_count_5))
    # counter.reply({})
    counter.reply(user_count_50)
    counter.close()

if __name__ == '__main__':
    main()
