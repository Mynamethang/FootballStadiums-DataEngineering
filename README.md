# FootballStadiums Data Engineering

## Project Description:

In this project, I implemented the Airflow for efficent developing, scheduling, and monitoring my Extract-Transform-Load pipeline. Airflow.
In addition, I deployed Airflow on Docker to make it run fast and easier.

> About the dataset : For this project, the data is scraped from Wikipedia.

 <img src='project-image\project-idea.png' width =700>

 ## 1. Extraction

 I utilized python packages such as bs4 and requests to parse the HTML content of the Wikipedia page

 ## 2. Transformation

 Using a popular data manipulation library is Pandas to perform transformations such as cleaing, formating datatype, entrich data.
 Then store data as CSV file on local machine.

 <img src='project-image\dataset.png' width =700>

 ## 3. Loading

Sqlachemy is a easy-to-use library that I used to insert tabular transformed data to PostgresSQL database.
Perfrom basic SQL querying statements in PostgreSQL.

 <img src='project-image\data_in_postgres.png' width =700>
 
## 4. Worflow

Run a pipeline on Ariflow for automatic sheduling and trigger .


 <img src='project-image\ETL-workflow.png' width =700>

## 4. Visualization

After that, I loaded dataset to PowerBI and create a Olympic Data Dashboard that visualize my  dataset.

<img src='project-image\dashboard.png' width =700>
