# SparkConf, SparkContext lets us create RDD and do stuffs with it
from pyspark import SparkConf, SparkContext

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
    return (int(fields[1]), (float(fields[2]), 1.0))

if __name__ == "__main__":
    # To create RDD
    conf = SparkConf().setAppName("WorstMovies")
    sc = SparkContext(conf = conf)

    # Load our movieID: movieName table
    movieNames = loadMovieNames()

    # Load raw u.data file -> Create RDD
    lines = sc.textFile("hdfs:///user/maria_dev/ml-100k/u.data")

    # Convert to (movieID, (rating, 1.0))
    movieRatings = lines.map(parseInput)

    # Reduce to (movieID, (sumOfRatings, totalRatings))
    # reduceByKey: Combine 2 values given by a key
    ratingTotalAndCount = movieRatings.reduceByKey(lambda movie1, movie2: (movie1[0]+movie2[0], movie1[1]+movie2[1]))

    # Map to (movieID, averageRating)
    # takes each value in ratingTotalAndCount
    averageRatings = ratingTotalAndCount.mapValues(lambda totalAndCount: totalAndCount[0] / totalAndCount[1])

    sortedMovies = averageRatings.sortBy(lambda x:x[1])

    # Take top 10
    results = sortedMovies.take(10)

    for result in results:
        print(movieNames[result[0]], result[1])
