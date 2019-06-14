from starbase import Connection

c = Connection("127.0.0.1", "8000")

ratings = c.table('ratings')

if (ratings.exists()):
    print("Dropping existing ratings table\n")
    ratings.drop()

ratings.create('rating') # Creates Column Family on a given table

#-----------------------------------
print ("Parsing ml-100k ratings data...\n")
ratingFile = open("/home/maria_dev/ml-100k/u.data", "r")

batch = ratings.batch() # starbase interface has batch function

for line in ratingFile:
    (userID, movieID, rating, timestamp) = line.split()
    batch.update(userID, {'rating': {movieID: rating}})

ratingFile.close()

#-----------------------------------
print("Committing ratings data to HBase via REST service\n")
batch.commit(finalize=True)

#-----------------------------------
print("Get back ratings for users.\n")
print("Ratings for user ID 1: \n")
print(ratings.fetch("1"))

ratings.drop()
