# Data Warehouse for Sparkify
This project uses IaC on AWS to create a datawarehosue for analyizing user activity on Sparkify by converting multiple log data files into a relational database using PostGreSQL.  

## Input Data
The input data consists of a set of json files storing information on the song and artist and a second set of json files contain log information on user activity (such as when they listen, the sequence of songs, the browser they use,...). The data is stored on AWS S3. 

## ETL Pipeline
The ETL pipeline is based on two steps. First, we create empty SQL tables. Second, we load data into it. To do so, we launch a redshift database as laid out in `ExploreDataStructures.ipynb`. Here, we also explore the data manually. Next, we start the console and execute `python create_tables.py`. This will delete tables if any exist and create empty SQL Tables. Running `python etl.py` will populate those. The SQL queries for both `python create_tables.py` and `python etl.py` are stored as doc strings in `sql_queries.py`. The raw data is first copied into staging tables. From there, it is loaded into the star schema SQL tables, with one fact table containing the individual song plays and 4 dimension tables:

Fact Table
   - songplays - records in event data associated with song plays i.e. records with page NextSong
         songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables

   - users - users in the app
        user_id, first_name, last_name, gender, level
   - songs - songs in music database
        song_id, title, artist_id, year, duration
   - artists - artists in music database
        artist_id, name, location, lattitude, longitude
   - time - timestamps of records in songplays broken down into specific units
        start_time, hour, day, week, month, year, weekday

## Run the scripts:
### 1) Create database cluster on AWS.
Launch a cluster as explained in `ExploreDataStructures.ipynb`. Note that the config file `dwh_local.cfg` is NOT provided as to not spoil my AWS credentials. The ARN And DWH role are copied to `dwh.cfg here`. The Juypter file is for exploration. 

### 2) Execute scripts
Launch a console and run:
- `python create_tables.py` 
- `python etl.py` 


## Authors 
[Jannic Cutura](https://www.linkedin.com/in/dr-jannic-alexander-cutura-35306973/), 2020

[![Python](https://img.shields.io/static/v1?label=made%20with&message=Python&color=blue&style=for-the-badge&logo=Python&logoColor=white)](#)




