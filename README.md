## Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The purpose of this project is to build an ETL pipeline that extracts data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for an analytics team to continue finding insights in what songs their users are listening to. The implementation of this involves loading data from S3 to staging tables on Redshift and executing SQL statements that create the analytics tables from these staging tables.

Queries given by the client's analytics team are run against the project results in order to test whether or not the database and ETL pipeline meet client expectations.


## DATABASE SCHEMA:
This project is designed using the star schema, which lends itself well to optimized query performance and simple designe that can easily be applied and utilized in various business applications.

#### TABLES:
##### FACT TABLES:
- songplays - records in event data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
##### DIMENSION TABLES:
- users - users in the app
user_id, first_name, last_name, gender, level
- songs - songs in music database
song_id, title, artist_id, year, duration
- artists - artists in music database
artist_id, name, location, lattitude, longitude
- time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday