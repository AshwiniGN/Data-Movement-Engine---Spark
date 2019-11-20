# Data-Movement-Engine

This project helps in pulling data from one source and load the data to another source and do basic data validation on record .

We deal with large amount of data where we need to move data from one source to another to perform logics or analytics. 
This projects helps in moving data with few simple configurations.

Basic data validation done between source and target:
Total records count
Distinct records count
columns count
Total null records count

### Prerequisites

1.Docker for Windows/Linux/Mac 
2.pull the spark container from the repository
docker pull dockerimage964/data_movement_spark
3. pull the mysql image and start the mysql
4. Intellij 

### Installing
docker pull dockerimage964/data_movement_spark
docker pull dockerimage964/mysql

## Running the tests

1. start the spark docker image
docker run -it -p 8088:8088 -p 8042:8042 -p 4040:4040 -h sandbox dockerimage964/data_movement_spark bash

2. start the mysql docker image
docker pull dockerimage964/mysql

docker run --name ms3 -p 3306:3306 -e MYSQL_ROOT_PASSWORD=password dockerimage964/mysql

docker exec -it ms3 bash

mysql -u root -ppassword

use mydb;

create table :

CREATE TABLE IF NOT EXISTS movie(
  adult BOOLEAN NOT NULL,
  id INT NOT NULL,
  original_title VARCHAR(300) NOT NULL,
	popularity DOUBLE(10,3),
	video BOOLEAN NOT NULL,
	PRIMARY KEY (id)
);

3. copy jars,log4j.properties file to spark docker image
4. place your file in S3 bucket and note down the access and secret key
Ex param file :
{
  "sourceType": "S3",
  "sourceAccessKey": "<access key>",
  "sourceSecretKey": "<seceretkey>",
  "sourceBucketPath": "dataanalysis100",
  "sourceFileName": "json",
  "sourceMultiLineFlag": "false",
  "targetType": "MYSQL",
  "targetIP": "<IP>",
  "targetPort": "3306",
  "targetSchemaName": "mydb",
  "targetTableName": "movie",
  "targetUsername": "root",
  "targetPassword": "password",
  "targetSaveMode": "overwrite"
}
  
  Run the spark submit command :
  Ex:
  spark-submit --jars /usr/local/share/spark/JARS/mysql-connector-java-8.0.18.jar,/usr/local/share/spark/JARS/hadoop-aws-2.6.0.jar,/usr/local/share/spark/JARS/aws-java-sdk-1.7.4.jar,/usr/local/share/spark/JARS/log4j-1.2.17.jar,/usr/local/share/spark/JARS/commons-csv-1.3.jar,/usr/local/share/spark/JARS/slf4j-api-1.7.21.jar --class com.spark.movedata.FetchAndMoveData --files /usr/local/share/spark/log4j.properties --conf spark.storage.memoryFraction=0.6 --conf spark.shuffle.memoryFraction=0.2 --conf spark.yarn.executor.memoryOverhead=4048 --conf spark.yarn.am.memory=5g --conf spark.driver.extraJavaOptions='-Dlog4j.configuration=file:/usr/local/share/spark/log4j.properties' --executor-memory 10g  --driver-memory 10g /usr/local/share/spark/JARS/FetchAndMoveData-0.0.1-SNAPSHOT.jar /usr/local/share/spark/config/s3tomysql.param FETCHANDMOVE
  
  you should be able to see the data in your mysql db once the job finishes.

##Note

You can add more source systems by creating a new method for each of the source. please refer to the code.

## Built With

* [Intellij] - IDE Used
* [Maven](https://maven.apache.org/) - Dependency Management
* [Environment] - docker

## Authors

* **Ashwini G N** - *Initial work* - [ashwininagataj964@gmail.com](https://github.com/AshwiniGN)

## Acknowledgments

* docker.hub.com
* stackoverflow.com





