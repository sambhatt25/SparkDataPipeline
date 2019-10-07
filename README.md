# A spark project developed in scala
#This project is to solve the problem with below scenarios
#FooBank, a fictional financial services company, based in Athens, Greece, which earns money by lending money to people. FooBank is quite successful and are paying out 10 loans per day. In order to better target their marketing efforts FooBank wants to be able understand which markets their products are the most popular in. Unfortunately the data has been spread out between a number of different microservices, which all expose data in different formats. FooBank's data scientists while otherwise quite competent are not familar enough with the HTTP protocol and need some help designing a data-pipeline, that joins those different tables into one table of observations. Steven who has been working at FooBank for a long time tells you that some of the status fields were broken during 2018 because of a bug.

#They are now asking you to design a data-pipeline to process and prepare the data for the data-scientists. They expect that the result of the pipeline is a single .csv file containing all available information for each loan including visits and customer data.

#Step 1: to execute the application please set the config properly before build.
#Edit the conf file (/SparkProj/src/main/resources/application.conf) and set the properties like below
#Set the master as per your need
#Then which functions you wish to execute
#Provide the file paths accordingly
#Clone the project
```
git clone https://github.com/sambhatt25/SparkDataPipeline
```

##Step 2: To start with: please extract the module and navigate to project directory SparkProj
###Run the below command, make sure you have mvn installed and spark installed in your system
###I have opted for skipTests because the sample files are given with my local directory
```
mvn package -skipTests
```


##Step 3: find the jar file created on successful build in the target directory of the project



##Step 4: Execute application with below command: 
###You can configure i,e --executor-cores 2 --executor-memory 16G as per your custer configurations 
```
spark-submit --class com.FooBank.SparkDataPipeline.CustomerLoans SparkDataPipeline-0.0.1-SNAPSHOT.jar
```

###Find the results as per given functions
