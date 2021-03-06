# Udacity Data Engineering Capstone Project Write Up

## About 
This `README` contains the write up for my Udacity Data Enigeering Nano Degree Program's capstone project `fundmappeR`. 
The codes associated with the project can be found at the project's github repo [here](https://github.com/JannicCutura/fundmappeR). 



## Scope of the Project
This project provides open source access to money market funds (MMF) portfolio data. 
MMFs have been at the center of the great financial crisis
([Chernenko and Sunderam, 2014][Chernenko2014];
[Gorton and Metrick, 2012][Gorton2012]), the sovereign
debt crisis ([Corre et. al., 2012][correa2012]) and recently experienced some turmoil during 
the Covid-19 pandemic ([Reuters, 2020][reuters2020]) in March 2021. 

Research on MMFs received a lot of attention, yet the barrier to enter the field is quite high since there is no off the shelve data available. 
The [SEC](https://www.sec.gov/) is collecting and publishing MMFs portfolios, but those are stored on 
their servers in an inconvenient format. `fundmappeR` parses the 
[SEC's website](https://www.sec.gov/open/datasets-mmf.html) 
for money market fund portfolio data and provides the data in an easily accessible format. 
The tables are updated every month as MMFs report on a monthly basis. 


## Project Architecture
This project is build on Amazon Web Services (AWS). AWS makes it easy to automate the downloading, parsing and cleaning of the data. 
The data pipeline is run every month to fetch and add the latest data. 

### Architectucal choices
The architectural choices were guided by the following key requirements:
 1. MMFs report only once every month. Running permanent provisioned hardware should be minized given it will be idle most of the time, but incur cost.
 2. Given the many steps of the ETL pipeline it should be automated completely. 
 3. Given the large data at hand, the final output shall be saved in a cheap but durable solution. 

The requirements are served by a combination of a largely serverless architecture (using AWS Lambda and Glue) and cheap, durable object storage (AWS S3). 
Lambda functions can be "triggered" using events to run arbitrary code and vanish after the code is completed. AWS Glue is an ETL orchestration service 
that allows to run spark jobs. These can be started using events as well. The final dataset is arround 70GB which makes it costly to host it in a dataware house
or even an relational database (such as RDS). I don't expect many requests to the data at this stage, but if they are requested, researchers will usually need 
all data for a certain range of years. Therefore the final data tables are stored in S3 in parquet format (which saves space) partitioned by year. 


### ETL steps
In a first step, a lambda function checks the SEC website for new funds
and sends a notification if new funds are found. Next, an EC2 instance (trigger by an Amazon Event Bridge Event) runs R code that downloads the raw reports to S3. Any S3 object "put" action serves
as an event trigger for AWS lambda, which picks up the raw filing, parses its XML structure and creates four tables for a given fund-month report. 
These are stored in on S3, partitioned by year. AWS Glue crawler populates the data catalog, used for Adhoc queries in Athena. A Glue ETL 
job runs a pyspark script which transform the individial csv files into parquet tables and stores them in a public S3 bucket to make
the data available to the user. 
![](https://github.com/JannicCutura/fundmappeR/blob/main/docs/fundmapper.png) 


### Quality Checks
Unfortunately, it can occasionally happen that funds do not report all information. Also, the format of the filings changed over the
past few years (from `N-MFP` to `N-MFP1` to `N-MFP2`). The Lambda function that parses the XML structure for any given report  and creates four tables 
is checking the format and applying certain transformation to ensure a consistent data model independently. Similarly, proper encoding 
of data types is also enfored in the Lambda code. 

### Data model
Researchers will  typically use this data to study MMF behviour. They will want to be able to easily join tables and aggregate along
different dimensions to reshape the data for the specific regression setup that they intend it for. Given the analytical use case of the data,
a relational data model was chosen to represent the data. More specificially, there are four tables:

- **Class table**: This table contains data on the individual fund share class. `class_id` and `date` act as primary key.
- **Series table**: This table contains data on the individual fund (i.e. a series). `series_id` and `date` act as primary key.
- **Holdings table**: This table contains data on the individual holdings. `class_id`, `date` and `issuer_number` act as primary key.
- **Collateral table**: This table contains data on the collateral posted for secured items. `class_id`, `date` and `issuer_number` act as primary key.

The following diagram visualizes the relationships and shows a subset of the variables:
![](https://github.com/JannicCutura/fundmappeR/blob/main/docs/database_schema.png)



## Addressing Other Scienarios
### What if the data was increased by 100x? 
The largely serverless architecture will scale to any increased load in the future. Amazon's S3 can hold virtually unlimited data, therefore
storing the data will not become an issue. As for processing, a given report is usually 300KB in size, the largest one I found was 6MB. A hypothetical
increase to 600MB (=6MB*100) per report would still be well below the 10GB memory limit of the AWS Lambda function and below the 32GB memory of EC2 instance chosen (albeit that
could in principal be increased). The GLUE ETL script is powered by pyspark and would handle arbitrary data amounts (given enough Data Processing Units).

### What if the pipelines would be run on a daily basis by 7am every day? 
This would work well and in fact one would only need to change the Amazon Event Brdige's events to start a Lambda function to check the SEC's website for new filings
and the event to start the EC2 instance to run the `R` script to webscrape the SEC's website

### What if the database needs to be accessed by 100+ people? 
The data is made available on AWS S3 for researchers free of charge. AWS S3 can easily serve 100+ people in parallel. 



[Chernenko2014]: <https://academic.oup.com/rfs/article-abstract/27/6/1717/1598733?redirectedFrom=fulltext> "Mytitle"
[Gorton2012]: <https://www.sciencedirect.com/science/article/abs/pii/S0304405X1100081X> "Mytitle"
[Huang2011]: <https://www.sciencedirect.com/science/article/abs/pii/S104295731000029X>
[reuters2020]: <https://www.reuters.com/article/g20-markets-regulation/regulators-target-money-market-funds-after-covid-19-turmoil-idUSL8N2I22GO>
[correa2012]: <https://www.google.com/url?sa=t&rct=j&q=&esrc=s&source=web&cd=&cad=rja&uact=8&ved=2ahUKEwj469Sp3rfuAhUMPuwKHd-pCuUQFjABegQIBhAC&url=https%3A%2F%2Fwww.ecb.europa.eu%2Fevents%2Fpdf%2Fconferences%2Fexliqmmf%2Fsession3_Correa_paper.pdf%3F1d92aade465b2b883a1a51d1b11f7295&usg=AOvVaw1A00b7DY74n4bnX5s3QaGL>

## Authors 
[Jannic Cutura](https://www.linkedin.com/in/dr-jannic-alexander-cutura-35306973/), 2020

[![Python](https://img.shields.io/static/v1?label=made%20with&message=Python&color=blue&style=for-the-badge&logo=Python&logoColor=white)](#)
[![R](https://img.shields.io/static/v1?label=made%20with&message=R&color=blue&style=for-the-badge&logo=R&logoColor=white)](#)
