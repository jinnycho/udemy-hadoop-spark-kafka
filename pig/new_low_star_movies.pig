ratings = LOAD '/user/maria_dev/ml-100k/u.data'
	AS (userID:int, movieID:int, rating:int, ratingTime:int);

metadata = LOAD '/user/maria_dev/ml-100k/u.item' USING PigStorage('|')
	AS (movieID:int, movieTitle:chararray, releaseDate:chararray, videoRelease:chararray, imdbLink:chararray);

nameLookup = FOREACH metadata
	GENERATE movieID, movieTitle, ToUnixTime(ToDate(releaseDate, 'dd-MMM-yyyy')) AS releaseTime;

ratingsByMovie = GROUP ratings by movieID;

avgRatings = FOREACH ratingsByMovie
	GENERATE group AS movieID, AVG(ratings.rating) AS avgRating;

lowStarMovies = FILTER avgRatings BY avgRating < 2.0;

lowStarsWithData = JOIN lowStarMovies BY movieID, nameLookup BY movieID;

newestLowStarMovies = ORDER lowStarsWithData BY nameLookup::releaseTime DESC;

DUMP newestLowStarMovies;
