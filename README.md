# Data Pipeline case study in Spark Scala
#### FooBank, a fictional financial services company, based in Athens, Greece, which earns money by lending money to people. FooBank is quite successful and are paying out 10 loans per day. In order to better target their marketing efforts FooBank wants to be able understand which markets their products are the most popular in. Unfortunately the data has been spread out between a number of different microservices, which all expose data in different formats. FooBank's data scientists while otherwise quite competent are not familar enough with the HTTP protocol and need some help designing a data-pipeline, that joins those different tables into one table of observations. Steven who has been working at FooBank for a long time tells you that some of the status fields were broken during 2018 because of a bug.

#### They are now asking you to design a data-pipeline to process and prepare the data for the data-scientists. They expect that the result of the pipeline is a single .csv file containing all available information for each loan including visits and customer data.

## Assumptions made during the development
   1) To get the overall view Loans are considered as key driver. Then Customer and then visits.
   2) There is blank column name present in Loans, renamed as sl_no
   3) Similarly, id column in customer renamed to customer_id, id in visits renamed to visit_id, timestamp column in visits renamed to visit_timestamp
   4) As analyzed, loan's user_id and customer's id are matched to use as join key
   5) Customer's webvisit_id and visit's id matched as join key.
   6) All the joins are made as left join to get a proper picture.
   
### Prerequisits to execute the steps below:
   1) Maven is installed in your environment
   2) Spark is installed with 2+ version to execute spark-submit
   3) Java version 1.8 is there in the system 
   
## Step 1: to execute the application please set the config properly before build.
#### Edit the conf file (/SparkDataPipeline/src/main/resources/application.conf) and set the properties like below
#### Set the master as per your need
#### Then which functions you wish to execute
#### Provide the file paths accordingly

### Clone the project
```
git clone https://github.com/sambhatt25/SparkDataPipeline.git

cd SparkDataPipeline
```

## Step 2: Build the code and package to jar file
### Run the below command, make sure you have mvn installed and spark installed in your system
```
mvn clean package
```

## Step 3: find the jar file created on successful build in the target directory of the project
```
ls target/*.jar
```

## Step 4: Execute application with below command: 
### You can configure i,e --executor-cores 2 --executor-memory 16G as per your custer configurations 
```
spark-submit --class com.FooBank.SparkDataPipeline.CustomerLoans target/SparkDataPipeline-0.0.1-SNAPSHOT.jar
```

### Find the csv file in the execution location as CustomerLoansOvservations.csv file

