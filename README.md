Multinational Retail Data Centralisation


The project obtains data from a number of different sources and via pandas/python, processes and cleans the data then uploads it to a SQL DB. 
The DB was/has to be corrected for types and P/F keys must be added.
The last stages is/was to create relation data between the tables in the data base, with the orders_table as the junction.

I learnt much more about data cleaning, data sampling, and how to spot bad data in a data set easily. I also learnt that knowing what the data will be used for is very important, some numbers did not match with the queried numbers during milestone 4, this was purely because I wiped rows that had None/NaN/Null etc, out of lack of knowledge and as a safety measure. Looking back I did not need to wipe as many rows as I did, and would have spent longer carefully taking out selective rows and a greater use of regex on certain data (although this is time consuming). 

Installation instructions

Git clone the project. Open in editor, then enter pip install in the terminal. If this doesnt download the require dependencies, the 3 files will list what you need to install. You will have to manually enter each - eg. pip install pandas etc. 

Make sure that the correct DB information is used, as the code requires a DB connection, both to obtain the data and to write the data. Pgadmin is recommended.

Usage instructions
The code use OOP, the commented out code at the bottom should serve as a guide.

File structure of the project
The file structure is not ideal but intended for easy of use for assessment reasons. Normally, each milestone shoould be in a sep. folder.
