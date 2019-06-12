# Spark 2 style
# SparkSession encompasses both SparkContext, SqlContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

# Find the lowest-rated movies that with at least 10 ratings

'''
Creates dictionary {movieID: movieName}
'''
def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

'''
Take each line of u.data
and convert it to tuple (movieID, (rating, 1.0))
'''
def parseInput(line):
    fields = line.split()
    return Row(movieID = int(fields[1]), rating = float(fields[2]))

if __name__ == "__main__":
    # To create RDD
    conf = SparkSession.builder.appName("WorstMovies").getOrCreate()

    # Load our movieID: movieName table
    movieNames = loadMovieNames()

    # Load raw u.data file -> Create RDD
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.data")

    # Convert to (movieID, (rating, 1.0))
    movieRatings = lines.map(parseInput)

    # Convert to a DF
    movieDataset = spark.createDataFrame(movieRatings)

    ratingsCount = movieDataset.groupBy("movieID").count().filter("count > 10")

    # Take top 10
    results = averagesAndCounts.orderBy("avg(rating)").take(10)

    for result in results:
        print(movieNames[result[0]], result[1], result[2])

    spark.stop()
