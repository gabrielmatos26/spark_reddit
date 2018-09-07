from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("RedditSentimentAnalysis") \
        .getOrCreate()
sc = spark.sparkContext
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
from pyspark.sql import *


from os import listdir
from os.path import isfile, join
import sys


def postSchema():
    postSchema = StructType()\
            .add("author","string")\
            .add("body", "string")\
            .add("score", "long")\
            .add("subreddit", "string")\
            .add("edited", "boolean")\
            .add("ups","long")\
            .add("controversiality", "long")\
            .add("created_utc", "timestamp")\
            .add("parent_id", "string")\
            .add("subreddit_id", "string")\
            .add("id", "string")\
            .add("author_flair_text", "string")\
            .add("gilded", "long")\
            .add("link_id", "string")\
            .add("retrieved_on", "timestamp")\
            .add("author_flair_css_class","string")\
            .add("distinguished", "string")
    return postSchema


def sentimentAnalysisUDF(msg):
    from nltk.sentiment.vader import SentimentIntensityAnalyzer
    from nltk import tokenize
    sid = SentimentIntensityAnalyzer()
    finalScore = 0
    words = tokenize.sent_tokenize(msg)
    for word in words:
        ss = sid.polarity_scores(word)
        wordScore = ss['compound']
        finalScore += wordScore
    msgLen = len(words)
    if msgLen == 0:
        msgLen = 1
    roundedFinalScore = finalScore/msgLen
    return roundedFinalScore

def fixEncodingUDF(msg):
    import re
    fixedStr = re.sub(r"[\\xc2-\\xf4][\\x80-\\xbf]+",
                      lambda match: match.group(0).encode('iso-8859-1').decode('utf8'), msg)
    return fixedStr

if __name__ == "__main__":
    if len(sys.argv) < 1:
        print("Usage: RedditSentimentAnalysis diretorio")
        sys.exit()

    pathToFiles = sys.argv[1]

    sentimentAnalysis = udf(sentimentAnalysisUDF, returnType=FloatType())
    fixEncoding = udf(fixEncodingUDF, returnType=StringType())

    pSchema = postSchema()
    reddit = spark.readStream.schema(pSchema)\
            .json(pathToFiles + "/*.json")
    datasetReddit = reddit.filter(reddit.subreddit == "StarWarsBattlefront")
    sa = datasetReddit.select("created_utc", "author", "id", "subreddit", round(sentimentAnalysis(fixEncoding("body")),4)\
                        .alias("sentiment_score"))
    saAvg = sa.withWatermark("created_utc", "24 hours").groupBy(window("created_utc", "24 hours"),"subreddit")\
                .avg()
    saFiltered = saAvg.select("window.start", "subreddit", saAvg["avg(sentiment_score)"].alias("avg_sentiment"))
    # saFiltered = saAvg
    saFiltered.printSchema()
    saFiltered.show()
    stream = saFiltered.writeStream\
                        .outputMode("append")\
                        .format("csv")\
                        .option("delimiter", ",")\
                        .option("path", pathToFiles + "/posts")\
                        .option("checkpointLocation", pathToFiles + "/checkpoint")\
                        .start()
                        # .option("header", "true")\
                        # .format("org.elasticsearch.spark.sql")\
                        # .option("checkpointLocation", "/save/location/sentiment")\
                        # .option("org.elasticsearch.spark.sql.resource", "reddit/reddit_sentiment")\
                        # .start("reddit/reddit_sentiment")
    stream.awaitTermination()
