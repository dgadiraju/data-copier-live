# Data Copier
A simple pipeline to copy data from a database (MySQL) to another database (Postgres).

## Setup Docker

## Setup MySQL

## Setup Postgres

## Run Application

## Setup AirFlow

Let us setup AirFlow and develop the pipeline for Data Pipeline.

#### Using Sqlite
Let us understand how to setup AirFlow for development using sqlite.
* AirFlow is Python based Library and hence we can install using `pip`.
* Let us create virtual environment for AirFlow and then setup AirFlow.
* Create directory by name **airflow** - `mkdir airflow`. Get into the directory by running `cd airflow`.
* Here is the command to create virtual environment - `python -m venv airflow-env`.
* Activate the virtual environment - `source airflow-env/bin/activate`
* Install AirFlow - `pip install apache-airflow`.
* Run `airflow initdb` to intialize the database and add configuration files. All the databases and configuration files will be created in our working directory **airflow**.
* By default it uses **sqlite** database.
* Run following commands to start airflow webserver and scheduler.
```
airflow webserver -p 8080 -D
airflow scheduler -D
```
* Then you can go to **http://ipaddress:8080**

Here are some of the disadvantages.
* Scalability
* Useful for development and evaluate AirFlow Features
#### Using MySQL
In Non Development environments we have to setup AirFlow using traditional RDBMS Databases.
Let us understand how we can configure AirFlow with **MySQL Database** and also using **LocalScheduler**.
* Make sure to stop all the airflow processes.
```
cat airflow-scheduler.pid | xargs kill
cat airflow-webserver.pid|xargs kill
ps -ef|grep airflow # if you find any outstanding processes kill using kill command
```
* Install **mysql-connector-python** so that we can use MySQL Database - `pip install mysql-connector-python`.
* Make sure **MySQL** database is setup.
```
docker run \
    --name mysql_airflow \
    -e MYSQL_ROOT_PASSWORD=itversity \
    -d \
    -p 4306:3306 \
    mysql
```
* Connect to **MySQL** and create database as well as username for airflow database - `docker exec -it mysql_airflow mysql -u root -p`
```
CREATE DATABASE airflow;
CREATE USER airflow IDENTIFIED BY 'itversity';
GRANT ALL ON airflow.* TO airflow;
FLUSH PRIVILEGES;
```
* Set executor to **LocalExecutor**.
```
executor = SequentialExecutor
```
* We can also use other Executors.
* Update **sql_alchemy_conn** with MySQL URL.
```
sql_alchemy_conn = mysql+mysqlconnector://airflow:itversity@localhost:4306/airflow
```
* Make sure some of the properties related to concurrency is adjusted to lower numbers.
```shell script
parallelism = 8
dag_concurrency = 4
max_active_runs_per_dag = 4
workers = 4
worker_concurrency = 4
worker_autoscale = 4,2
```
* Run `airflow initdb` to initialize MySQL Database.
* Start **webserver** and **scheduler** in the background.
```
airflow webserver -p 8080 -D
airflow scheduler -D
```
* Then you can go to **http://ipaddress:8080**

We can switch over to **CeleryExecutor** by following these steps.
* Install Celery using `pip install apache-airflow['celery']`
* Change executor to CeleryExecutor
```
executor = CeleryExecutor
```
* Kill all the webserver and scheduler processes.
```shell script
ps -ef|grep scheduler|awk -F" " '{ print $2 }'|xargs kill -9
cat *pid|xargs kill -9
rm *pid
ps -ef|grep airflow #Kill any remaining sessions
```
* Start the airflow components and validate by visiting the URL.
```
airflow webserver -p 8080 -D
airflow scheduler -D
```
## Schedule using AirFlow
By this time we should be ready with our application as well as AirFlow. Let us understand how we can integrate both.
* As we are going to run our applicatio using Docker Container, we will use DockerOperator provided by AirFlow.
* Add Docker to AirFlow - `pip install apache-airflow['docker']`