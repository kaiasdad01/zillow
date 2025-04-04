In this project, I am combining data from Zillow Research & live API feeds. 

## Zillow Research Data
These are CSV files produced by Zillow showing historical data. I am using a Lambda to download all of these files and stream them to s3. This process will be scheduled to align with Zillow's regular monthly update, which occurs on the 16th of each month. 

TODO - add validation to ensure that csv is not blank. This is to help ensure that the csv path has remained consistent, as Zillow mentions they may adjust csv path in future. 

