import sys
import os
sys.path.append(os.path.dirname(os.path.abspath('main.py')))
from mapper import *

def main():
    mapper = FunnyMapper('map', 'reviews', 'funny')
    mapper.start_consuming(bind_first=False)
    mapper.close()

if __name__ == '__main__':
    main()
