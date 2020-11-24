import sys
import os
sys.path.append(os.path.dirname(os.path.abspath('main.py')))
from consumers import *

def main():
    querier = CommentQuerier(keyIds = ['user_id', 'text'], exchange = 'reviews', routingKey = 'comment')
    LastCommentCountPerUser = querier.count()
    allSameCommentReviewsPerRelevantUser = querier.join(LastCommentCountPerUser)

    print(len(LastCommentCountPerUser), " Users")
    print(len(allSameCommentReviewsPerRelevantUser), " Relevant users always commenting the same")
    querier.reply(allSameCommentReviewsPerRelevantUser)
    querier.close()

if __name__ == '__main__':
    main()
