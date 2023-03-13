# dwh-load-facilites-spark-app
A java spark app that loads all facilities to the DWH ODS.

##  Requirements
The following are required in order to run the project:

- Java 8
- Maven

Create the required jar file using maven.
Be sure to remove the spark libraries from the scope as these will be provided in the cluster.

```bash
$ mvn clean install
```

Submit the application to begin the job. Note that this command should be run from the desired spark driver.

```bash
./bin/spark-submit --class LoadFacilities \
--master spark://<spark-master>:7077 \
--deploy-mode client \
--conf spark.source.database-name='<db_name>' \
--conf spark.source.metadata-table='<table_name>' \
--conf spark.source.database-host='<db_host>' \
--conf spark.source.url='jdbc:mysql://<db_host>/<db_name>' \
--conf spark.source.driver='com.mysql.cj.jdbc.Driver' \
--conf spark.source.user=<db_user> \
--conf spark.source.password=<db_password> \
--conf spark.source.numpartitions=18 \
--conf spark.sink.url='jdbc:sqlserver://<db_host>;encrypt=false;databaseName=<db_name>' \
--conf spark.sink.driver='com.microsoft.sqlserver.jdbc.SQLServerDriver' \
--conf spark.sink.user=sa \
--conf spark.sink.password='<password>' \
--conf spark.sink.dbtable='<sb_table>' \
load-all-facilities-1.0-SNAPSHOT-jar-with-dependencies.jar
```