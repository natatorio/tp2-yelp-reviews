import sys
import os
sys.path.append(os.path.dirname(os.path.abspath('main.py')))
from counter_by import *

def main():
    joiner = Stars5JoinerCounterBy(keyIds = ['user_id'], exchange = 'reviews', routingKey = 'stars5')
    user_count_stars5 = joiner.count()
    users_50_stars5 = joiner.join(user_count_stars5)

    print(len(user_count_stars5), " Us 1 o + revs 5 stars")
    print(len(users_50_stars5), " Us 50 o + revs todas 5 stars: ")
    joiner.reply(users_50_stars5)
    # joiner.reply(users_50_stars5)
    joiner.close()

if __name__ == '__main__':
    main()
