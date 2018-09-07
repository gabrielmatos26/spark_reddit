# -*- coding: utf-8 -*-
##
## Based on the original script from https://github.com/dewarim/reddit-data-tools/blob/master/src/main/python/scoreCommentsJson.py
##

from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk import tokenize

import bz2
import lzma
import json
import sys
import os.path

sid = SentimentIntensityAnalyzer()
archive = "data/RC_2007-10.bz2"
if len(sys.argv) > 1:
    # when started via "python scoreCommentsJson.py /data/location/reddit_comments.bz2, 
    # the first element of sys.argv seems to be the script name, not the path, like in Java.
    archive = sys.argv[1]
else:    
    print("No command line arguments given - trying to work with default example data file "+archive)
    

print("Working on file: " + archive)
score_file_name = archive.replace("json", "sentiment")
if os.path.exists(score_file_name):
    print("sentiment file already exists")
    sys.exit()

# bz_file = bz2.BZ2File(archive, 'rb', 1000000)
json_file = open(archive, 'rb')
score_file = open(score_file_name, 'w')

commentCount = 0
while True:
    line = json_file.readline().decode('utf8')
    if len(line) == 0:
        break
    comment = json.loads(line)
    if comment['subreddit'] == "StarWarsBattlefront":
        # print(comment)
        id = comment["id"]
        body = comment["body"]

        # variables to hold the overall average compound score for message
        finalScore = 0
        roundedFinalScore = 0

        # variables to hold the highest positive score in the message
        # and highest negative score in the message
        maxPosScore = 0
        maxNegScore = 0

        # print("===")
        commentLines = tokenize.sent_tokenize(body)
        for line in commentLines:
            ss = sid.polarity_scores(line)
            # uncomment these lines if you want to print out sentences & scores
            '''
            line = line.replace('\n', ' ').replace('\r', '')
            print(line)
            for k in sorted(ss):
                print(' {0}: {1}\n'.format(k,ss[k]), end='')
            '''
            lineCompoundScore = ss['compound']
            finalScore += lineCompoundScore

            if ss['pos'] > maxPosScore:
                maxPosScore = ss['pos']
            elif ss['neg'] > maxNegScore:
                maxNegScore = ss['neg']

        # roundedFinalScore is the average compound score for the entire message
        commentLength = len(commentLines)
        if commentLength == 0:
            commentLength = 1
        roundedFinalScore = round(finalScore / commentLength, 4)
        comment['sentiment'] = roundedFinalScore
        new_comment = json.dumps(comment)
        score_file.write("{0}\n".format(new_comment))
        commentCount += 1
        if commentCount % 1000 == 0:
            print(commentCount)
            # break
json_file.close()
score_file.close()
