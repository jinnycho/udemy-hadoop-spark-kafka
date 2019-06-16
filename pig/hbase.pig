/*
To create table beforehand
I ran
> create 'users', 'usersinfo'
where 'users' is the name of the table
and 'usersinfo' is the name of the column family
*/

users = LOAD '/user/maria_dev/ml-100k/u.user'
USING PigStorage('|')
-- relation, first column = key
AS (userID:int, age:int, gender:chararray, occupation:chararray, zip:int);

STORE users INTO 'hbase://users'
USING org.apache.pig.backend.hadoop.hbase.HBaseStorage (
'userinfo:age,userinfo:gender,userinfo:occupation,userinfo:zip');
