# Sparkify Song Play Analysis

## Introduction 
This is a learning project and not a real-world application. Use with caution. Sparkify does not exist. 

This project contains scripts to help Sparkify analytics team analyse song listening behavior.

## Schema 
Input data is fitted into 5 tables after ETL pipeline run:
1. songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

![songplays table](https://i.imgur.com/a4e4Qim.jpg)

2. users (user_id, first_name, last_name, gender, level)

![users table](https://i.imgur.com/GpDsGph.jpg)

3. songs (song_id, title, artist_id, year, duration)

![songs Table](https://imgur.com/SHUMYdX.jpg)

4. artists (artist_id, name, location, latitude, longitude)

![artists Table](https://i.imgur.com/TVCO492.jpg)

5. time (start_time, hour, day, week, month, year, weekday)

![time table](https://i.imgur.com/50dMGBg.jpg)

While tables 2-5 are used to organise data in dimensions (which users do we have, which songs, etc.), 
table 1. is a fact table designed for analytics. 
It summarizes data about listening behaviour and allows to have all necessary information in one table for better performance.  

### Data types
Most textual data types are chosen as free text strings without limit to accommodate different kinds of possible inputs. This is also a design choice, because in Postgres such strings shouldn't take much more space than they actually are. 

Also, timestamps from logs are transformed and persisted as proper ISO timestamps to ensure consistency. 


## Running the project

## Pre-requisites
1. There are two types of input data - log data and song data. They need to be put into respective 
directories under folder `data` to be consumed by the scripts. Please refer to example files to understand needed structure. 
2. Make sure you have Python installed, it is needed to run scripts.

## Process
To generate database and tables, you need to have open Terminal and be in parent directory of the project. 
1. Run `python create_tables.py` to (re)create database and table structure before importing new data. Please be aware this will remove any data imported before, so use with care. 
2. Run `python etl.py` to consume input song and data files and fill the tables created in step 1.

## Analytics
To see if the process has succeeded (apart from Terminal logs), open and run `analytics.ipynb` notebook. 
There you will see simplistic representation of songplay and general stats about other tables. 
If you want to see what is contained in tables in depth, consider also using `test.ipynb`, it shows more granular data about each of the created tables.

# License
Please refer to `LICENSE.md` to understand how to use this project for personal purposes.
