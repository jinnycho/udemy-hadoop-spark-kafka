DROP VIEW IF EXISTS topMovieIDs;

CREATE VIEW topMovieIDs AS
SELECT movieid, count(movieid) as ratingCount
FROM movie_ratings
GROUP BY movieid
ORDER BY ratingCount DESC;

SELECT n.title, ratingCount
FROM topMovieIDs t JOIN movie_names n ON t.movieid = n.movieid;
