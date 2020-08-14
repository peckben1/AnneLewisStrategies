# AnneLewisStrategies

This code relies on three input files 
(found at https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons.csv, 
https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email.csv, and 
https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email_chapter_subscription.csv)

For the code to function correctly, these files must be stored in a directory called "data" which is kept in the same directory as the code itself. 
They are Jupyter Notebook files, and as such will require Jupyter to run. They will also require Python 3 as well as the Pandas package and its prerequisites. 
ETL2.ipynb requires that ETL1.ipynb be first run in full. 

UPDATE: Added ETL.py, which allows the code to be run and the resulting files to be created without installation of Jupyter. Running ETL.py requires only Python 3 and Pandas and is the equivalent of running ETL1.ipynb and then ETL2.ipynb back to back, albeit without displaying any of the intermediary checks and comments from the work process. 
