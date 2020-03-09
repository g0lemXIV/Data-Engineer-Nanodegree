## Project: Data Modeling with Cassandra
![](https://bigdatadimension.com/wp-content/uploads/2017/04/cassandra.png)
### Overview
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions, and wish to bring you on the project. My role is to create a database for this analysis. I'll be able to test my database by running queries given to you by the analytics team from Sparkify to create the results.

### Project Details  
**Datasets**
For this project, I'll be working with one dataset: ```event_data```. The directory of CSV files partitioned by date. Here are examples of filepaths to two files in the dataset:
```bash
event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv
```
**Project Template**
The project template includes one Jupyter Notebook file, in which:  

- I processed the event_datafile_new.csv dataset to create a denormalized dataset  
- I modeled the data tables keeping in mind the queries you need to run  
- I provided queries that you will need to model your data tables for  
- I loaded the data into tables you create in Apache Cassandra and run your queries  






