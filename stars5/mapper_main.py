import sys
import os
sys.path.append(os.path.dirname(os.path.abspath('main.py')))
from mapper import *

def main():
    mapper = Stars5Mapper('map', 'reviews', 'stars5')
    mapper.start_consuming()
    mapper.close()

if __name__ == '__main__':
    main()
