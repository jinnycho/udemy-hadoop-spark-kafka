from mrjob.job import MRJob
from mrjob.step import MRStep

# Count up ratings given for each movie
class MoviesRatingsBreakdown(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings)
        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def reducer_count_ratings(self, key, values):
        yield key, sum(values)

# Sort the movies by their numbers of ratings
class MoviesRatingsBreakdownSort(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings),
            MRStep(reducer=self.sort_count_ratings)
        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def reducer_count_ratings(self, key, values):
        # make ratings_count as key so we can sort them by key
        # + movies all concatenated for the same num of ratings_count
        yield str(sum(values)).zfill(5), key

    def reducer_count_ratings(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    #MoviesRatingsBreakdown.run()
    MoviesRatingsBreakdownSort.run()
