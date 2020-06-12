# Data Engineering Capstone Project
======================================================

![udacity](https://www.udacity.com/www-proxy/contentful/assets/2y9b3o528xhq/2O4R1bvNrkqUoVM1bE5QxI/4180cfe807d20ca1af7bcb921f826196/SEO-Image-IR6.jpg)

#### In this project, I apply what I have learned on data engineering nanodegree course  
------------------------------------
As a startup company Sparkify specialized in streaming music, CEO's see new oportunity in podcasts listening. The team want to build podcasts database to take a closer look at this topic. As a Data Engineer I was asked to create database with [RRS feed](https://en.wikipedia.org/wiki/RSS), basic information about podcasts, episodes and podcaster names.  

This data helps analyst to get into podcasts topic, search the most common categories of podcasts and create raport which help engineers to implement functionality on the production to monitor users behavior.

### Project description
---------------------------
The project includes data from 3 different data sources:
  - Kaggle [dataset](https://www.kaggle.com/roman6335/13000-itunes-podcasts-april-2018) with +13 000 iTunes Podcasts updated at April 2018  
  - Json data from iTunes Search API with around +50 000 podcasts basic informations and RRS feed url  
  - +1 000 000 podcasts episodes and other information dataset obtained thanks to RSS feed parsing (initially in XML format)

**The purpose of this porject is to answer the following questions:**  
  - What is the average size and duration of a podcast?
  - How often do authors publish new podcasts?
  - What are the most popular podcast categories?

### Steps taken in the project  
A brief description of the steps that have been taken in the project
1. Downloading podcast data from kaggle.com.
2. Authors extraction and Itunes API search for new data (in this way over 50,000 podcasts were obtained, which is a 350% increase compared to the initial data).
3. Extract links to the RSS of each of the authors and searches the content for the data we are interested in. This means descriptions and names of episodes, genre, etc.
4. Data cleaning and processing.
5. Save the cleared data to s3.
6. Uploading data to the redshift database model.
------------------------------   

### The choice of tools, technologies, and data model  

#### Data model
![model](https://i.ibb.co/6J9VV70/Zrzut-ekranu-z-2020-06-12-00-09-27.png)

-----------------------------------------------------
Because I collected a lot of data (over 3 million podcast episodes), we chose redshift as the service. We choose Redshift because it powers analytical workloads for Fortune 500 companies, startups, and everything in between. Companies like Lyft have grown with Redshift from startups to multi-billion dollar enterprises. I also chose the star model where attached in the above drawing.
I used pandas and NumPy as well as libraries for multithreaded calculations for the initial data processing. I also used the open library [pyPodcastParser](https://github.com/mr-rigden/pyPodcastParser) which helped me process XML data. I used the requests library for iTunes queries, unfortunately, the public API version is limited to 20 queries per minute.
For storing the data set I used the S3 bucket there can also land data if the system will work in real-time.   

 ### ETL pipeline

[![](https://mermaid.ink/img/eyJjb2RlIjoiZ3JhcGggVERcbiAgQVtLYWdnbGUgZGF0YXNldF0gLS0-fGF1dGhvciBuYW1lc3wgQihBcHBsZSBpVHVuZXMgQVBJKVxuICBCIC0tPiB8dXJsIGZlZWR8IEMoWE1MIGZlZWQpXG4gIEEgLS0-fHJhdGluZ3N8IERbTGFwdG9wXVxuICBCIC0tPnxhdXRvaG9ycyBpbmZvcm1hdGlvbnN8IERbaVBob25lXVxuICBDIC0tPnxlcGlzb2RlcyBpbmZvcm1hdGlvbnN8IER7TG9jYWwgc3RvcmFnZX1cblx0RCAtLT58cHJvY2Vzc2luZyBkYXRhfCBFW0xvYWQgdG8gUzNdXG4gIEUgLS0-fHN0YWdpbmcgYW5kIGluc2VydGluZ3wgR1tMb2FkIHRvIEFtYXpvbiByZWRzaGlmdF0iLCJtZXJtYWlkIjp7InRoZW1lIjoiZGVmYXVsdCJ9LCJ1cGRhdGVFZGl0b3IiOmZhbHNlfQ)](https://mermaid-js.github.io/mermaid-live-editor/#/edit/eyJjb2RlIjoiZ3JhcGggVERcbiAgQVtLYWdnbGUgZGF0YXNldF0gLS0-fGF1dGhvciBuYW1lc3wgQihBcHBsZSBpVHVuZXMgQVBJKVxuICBCIC0tPiB8dXJsIGZlZWR8IEMoWE1MIGZlZWQpXG4gIEEgLS0-fHJhdGluZ3N8IERbTGFwdG9wXVxuICBCIC0tPnxhdXRvaG9ycyBpbmZvcm1hdGlvbnN8IERbaVBob25lXVxuICBDIC0tPnxlcGlzb2RlcyBpbmZvcm1hdGlvbnN8IER7TG9jYWwgc3RvcmFnZX1cblx0RCAtLT58cHJvY2Vzc2luZyBkYXRhfCBFW0xvYWQgdG8gUzNdXG4gIEUgLS0-fHN0YWdpbmcgYW5kIGluc2VydGluZ3wgR1tMb2FkIHRvIEFtYXpvbiByZWRzaGlmdF0iLCJtZXJtYWlkIjp7InRoZW1lIjoiZGVmYXVsdCJ9LCJ1cGRhdGVFZGl0b3IiOmZhbHNlfQ)

### Data Dictionary
**Data dictionary is available at this link:** [Data Dictionary Final Project](https://udacity-course-bucket.s3.us-east-2.amazonaws.com/Data+Dictionary+-+Final+Project.pdf)  

  
### Results
------------------------------------
Modeling results and results can be seen in the notebooks folder of the test.ipynb file
If you want to run a pipeline, you must have a properly configured environment, but for the sake of data security which in some countries may be confidential. Contact me, please.

### Next steps and improvement  
------------------------------------
**The data was increased by 100x**
If the data increases x100 times, it will be impossible to process them on one computing machine. For this purpose you will need to use BigData tools such as Spark, Hadoop, Athena.  

**The pipelines would be run on a daily basis by 7 am every day**
The next step for this project will be designing a solution based on Airflow, this will solve the problem of scheduling data at selected times and on selected days of the week. It will also allow monitoring of data flow and optimization.  

**The database needed to be accessed by 100+ people**  
The chosen solution (Redshift) will support up to 300-500 people ([link](https://medium.com/@Pinterest_Engineering/powering-interactive-data-analysis-by-redshift-b108c2ea9165)). Depending on how big a set of data we will have and how big a cluster we will manage. Optimizing the redshift service for performance will also be important.
