# Sparkify Data Warehouse

In this project, I applied what I learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, I load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

## Purpose of this ETL in the context of the Sparkify
 
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

I am tasked with building an ETL pipeline that extracts data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for analytics team to continue finding insights in what songs their users are listening to.

 **Rafal Bodziony: bodziony.rafal@gmail.com**.
## State and justify database schema design and ETL pipeline  

The goal of this project is to facilitate the analysis of song data and customer logs. We choose Redshift becouse it powers analytical workloads for Fortune 500 companies, startups, and everything in between. Companies like Lyft have grown with Redshift from startups to multi-billion dollar enterprises.

**ETL pipeline**  

![ ETL shema](https://mermaid.ink/img/eyJjb2RlIjoiZ3JhcGggVERcblx0QVtTb25nIGRhdGFdIC0tPnxwdXQgb24gUzN8IEJ7QVdTIFMzfVxuICBaW0xvZyBkYXRhXSAtLT58cHV0IG9uIFMzfCBCe0FXUyBTM31cblx0QiAtLT4gfGNvcHkgZnJvbSBTM3xDKFN0YWdpbmcgdGFibGUpXG5cdEMgLS0-fGluc2VydCBpbnRvfCBEW1N0YXIgc2NoZW1hIHRhYmxlc11cblx0RCAtLT58cXVlcnl8IEVbQW5hbHl0aWNzIHRlYW1dXG4gIFx0RCAtLT58cXVlcnl8IEVbQW5hbHl0aWNzIHRlYW1dXG4gICAgXHREIC0tPnxxdWVyeXwgRVtBbmFseXRpY3MgdGVhbV1cbiAgICAgIFx0RCAtLT58cXVlcnl8IEVbQW5hbHl0aWNzIHRlYW1dIiwibWVybWFpZCI6eyJ0aGVtZSI6ImRlZmF1bHQifSwidXBkYXRlRWRpdG9yIjpmYWxzZX0)



**Star schema of analytics Database**  

![Database schema](https://i.ibb.co/sW5kn3V/db-p-roject3.png)  

## How to run
To properly create a redshift database and run scripts, you need to correctly establish a database connection.
First You need to request for host endpoint and connection parameters. Add credentials to dwh.cfg as follow
```SQL
HOST='<cluster_name>.xxxxxxxx.<region>.redshift.amazonaws.com'
DB_NAME=<database_name> 
USER=<user_name> 
DB_PASSOWRD=<pass>
DB_PORT=<port>
```

Next, add IAM Role into dwh.cfg to establish a connection between S3 storage and Redshift
```SQL
ARN=arn:aws:iam::xxxxxxxxx:role/<name>
```
### REMEMBER! 
**To establish connection ask for creditentail and IAM Role.**


## Run ETL
To run ETL pipeline first create all tables with `create_tables.py`
```
user:~/project_dir$:python create_tables.py
```

Then the output should be:

```
user:~/project_dir$:config read
user:~/project_dir$:connenction establish
user:~/project_dir$:drop tables
user:~/project_dir$:create tables
user:~/project_dir$:Tables are ready to use in user=<user< password=xxx dbname=<db_name> host=<host> 
```

To insert data into database run `etl.py` script

```
user:~/project_dir$:python etl.py
```

Then You should see how many files was found, progress loop  and
information about success and processing time.