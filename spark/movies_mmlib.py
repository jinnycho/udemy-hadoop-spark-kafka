# Spark 2 style
# SparkSession encompasses both SparkContext, SqlContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import lit
from pyspark.ml.recommendation import ALS

'''
Creates dictionary {movieID: movieName}
'''
def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1].decode('ascii', 'ignore')
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
    lines = spark.read.text("hdfs:///user/maria_dev/ml-100k/u.data").rdd

    # Convert to Row (userID, movieID, rating)
    movieRatings = lines.map(parseInput)

    # Convert to a DF
    movieDataset = spark.createDataFrame(movieRatings).cache()

    # Create an ALS collaborative filtering model from dataset
    als = ALS(maxIter=5, regParam=0.01, userCol="userID", itemCol="movieID", ratingCol="rating")
    # Train my dataset with this recommendation model
    model = als.fit(movieDataset)

    # Print out ratings from user 0
    # User 0 will be the person who will be recommended
    print("\nRatings for user ID 0: ")
    userRatings = movieDataset.filter("userID = 0")
    for rating in userRatings.collect():
        print movieNames[rating['movieID']], rating['rating']

    print("\nTop 20 recommendations: ")
    # Limit to movie rated more than 100 times for more accuracy
    # create ratingsCounts df by grouping movidID -> count (so movieID: count) -> filter
    ratingCounts = movieDataset.groupBy("movieID").count().filter("count > 100")
    # new dataframe with movieID & userID & 0. will add predictions
    popularMovies = ratingCounts.select("movieID").withColumn("userID", lit(0))
    # Run model on that list of popular movies
    recommendations = model.transform(popularMovies)

    topRecommendations = recommendations.sort(recommendations.prediction.desc()).take(20)

    for rec in topRecommendations:
        print (movieNames[rec['movieID']], rec['prediction'])
    spark.stop()
