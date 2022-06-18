# Parking-Availability-Datasets
This repository provides code to preprocess openly available data regarding parking availability that can be used for scientific research. 
For every dataset there is a main.py that processes the data and a requirements.txt that has to be installed in advance. 
## Seattle
Seattle data is based on the annual parking study that is conducted by the deptartement of transportation seattle and pubvlished in open data portal [ https://data.seattle.gov/Transportation/Annual-Parking-Study-Data/7jzm-ucez ]. 
Moreover we consider the parking meter transactions that are published by the city
## Melbourne 
We load and process the in-ground sensor data published by the city of Melbourne [ https://www.melbourne.vic.gov.au/about-council/governance-transparency/open-data/Pages/on-street-parking-data.aspx ] The dataframe contains information when a car arrives and leaves at a given parking space. We outptu a time series with the parking availability lable of all street blocks every five minutes

## San Francisco
We process the parking meter transactions and the in-ground sensors of the SFPark project [ https://www.sfmta.com/getting-around/drive-park/demand-responsive-pricing/sfpark-evaluation ] and merge both datasets
