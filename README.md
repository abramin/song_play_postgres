The README file includes a summary of the project, how to run the Python scripts, and an explanation of the files in the repository. Comments are used effectively and each function has a docstring.

# SongPlay Postgres ETL Project

## Summary

This project provides ETL (Extract-Transform-Load) functionality for a songplay dataset. 

* The ***extraction*** phase loads data from 2 json files, 1 containing song & artist data, the other containing log data on users and related listening events.
* The extracted data is ***transformed*** to generate 5 dataframes - Songs, Artists, Users, Time and Songplay. Songplay is a compound dataset using fields from all other dataframes.
* The 5 dataframes are then ***loaded*** into a Postgres database. Foreign key constraints have been added to ensure referential integrity (e.g. between Songs and Artists tables).

## Files

* *sql_queries.py* : SQL statements defined as python variables from table creating, record inserts and song select.

* *create_tables.py* : Functionality to create the database as well as create and drop the tables defined in *sql_queries.py*

* *etl.py* : The etl logic to extract the data from the json files, transform it as needed and load it into the Postgres database.

* *etl.ipynb* : An interactive notebook that explains the etl code in more detail.

* *test.ipynb* : A notebook with some select queries provided to test the data has been loaded correctly to the database.

## How to run the code

! To run this code, you will need Python installed and a running postgres database instance.

 * In a terminal, navigate to the root folder and execute

 ```python
    $ python create_tables.py
    $ python etl.py
 ```

This will set up the database and run the etl code on the data.

After this you can test the data has been loaded using the test.ipynb code in Jupyter notebooks, or by running select queries directly in a Postgres DB interface.