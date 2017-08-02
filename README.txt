ETL OF ORIGINS DATA POINTS FROM MSBASE TO MSBIOSCREEN - July 10, 2017 edited August 2, 2017

The process of adding Origins patient data followed these steps:

1) Extract the data points using .sql statements executed on the msbase databases using phpmyadmin and outputting the query results to a .csv file.

2) Sanity checking the query results in excel.

3) Parsing and uploading the data to the database via the msbioscreen api using ruby scripts.


EXTRACTING DATA POINTS

Using the msbioscreen api documentation as a guide, I created sql queries to gather the datapoints for uploading to the subjects, visits, attacks, and treatments endpoints. These files are named extract_modelname.


SANITY CHECKING AND TRANSFORMING NULL VALUES

I manually spot checked values in the generated spreadsheets against the same data points in msbase to ensure accuracy. MySQL populates fields with a NULL value with \N. To ensure that a NULL value would be uploaded into the database, I used sed to replace \N with "" (empty string) ie: sed 's/\\N/""/g' filename > filename_reformatted. csv filenames ending in _reformatted.csv have been altered in this way.


PARSING AND UPLOADING DATA

To upload the resulting .csv files, I created simple ruby scripts to parse the csv and upload the data row by row. This was achieved using the CSV and HTTParty gems. Each script parses the csv row by row and executes a post request to the appropriate api endpoint for the data points involved. I tried some elementary error handling, but removed it as it was not working as intended. All data so far has uploaded without problem. The scripts to upload data all have names that begin with load_ followed by the model name of the data. In
most cases this is the same name as the api endpoint to post the data to. The load scripts may be run from a terminal command line, either in a ruby console (irb) or by executing with the ruby command (ruby load_visits.rb).


STILL TO DO

Automate the extraction/upsertion process to run several times an hour.
Find out why uploaded data points are not appearing in the bioscreen app (any version).

OTHER NOTES

This directory contains a .gitignore file to ensure that .csv files and vars.rb are not checked into github.

To use the load scripts, you will need to create a file called vars.rb in the same directory as the load scripts. This is where you should place variable data such as urls and token data. DO NOT check this file into github!

vars.rb variable format:

@acceptance_base_url = "https://path/to/acceptance/url"
@prod_base_url = "https://path/to/production/url"

@patients_url = "api/v1/subjects"
@visits_url = "api/v1/visits"
@attacks_url = "api/v1/attacks"
@treatments_url = "api/v1/treatments"

@acceptance_auth_token = "AUTH_TOKEN_GOES_HERE"
@prod_auth_token = "AUTH_TOKEN_GOES_HERE"

@working_dir_base = absolute path of the local directory containing the folder with the csv files
@working_dir = name of the directory containing the csv files to be loaded into the database



Note that the variable names are configured to work with the load scripts. The variable values are subject to change. Substitute your actual authorization token for AUTH_TOKEN_GOES_HERE.
