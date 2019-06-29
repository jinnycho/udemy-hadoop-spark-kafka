ratings = LOAD '/user/maria_dev/ml-100k/u.data'
	AS (userID:int, movieID:int, rating:int, ratingTime:int);

metadata = LOAD '/user/maria_dev/ml-100k/u.item' USING PigStorage('|')
	AS (movieID:int, movieTitle:chararray, releaseDate:chararray, videoRelease:chararray, imdbLink:chararray);

nameLookup = FOREACH metadata
	GENERATE movieID, movieTitle;

ratingsByMovie = GROUP ratings by movieID;

avgRatings = FOREACH ratingsByMovie
	GENERATE group AS movieID,
    AVG(ratings.rating) AS avgRating,
    COUNT(ratings.rating) AS numRating;

badMovies = FILTER avgRatings BY avgRating < 2.0;

badMoviesWithData = JOIN badMovies BY movieID, nameLookup BY movieID;

highlyRatedbadMovies =
	FOREACH badMoviesWithData GENERATE nameLookup::movieTitle AS movieName,
    badMovies::avgRating AS avgRating, badMovies::numRating AS numRating;

highlyRatedbadMoviesSort = ORDER highlyRatedbadMovies BY numRating DESC;

DUMP highlyRatedbadMoviesSort;
