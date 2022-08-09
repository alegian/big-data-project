# big-data-project
Python script to load test data into CassandraDB (noSQL database). It connects to a Datastax AstraDB cloud database.
## Test Data
The test data is downloaded from [here](https://www.kaggle.com/grouplens/movielens-20m-dataset). 
The ZIP file's contents should be extracted and placed directly in this project's root directory.
## Database Connection
The code tries to connect to a Datastax AstraDB cloud database. For this to work, you need the
"secure-connect" bundle ZIP (downloaded from the AstraDB dashboard). This file should be placed directly
in the project's root (not extracted!). You also need a `.env` file at the root, containing the values 
`DB_USER` and `DB_SECRET` (your database's credentials)