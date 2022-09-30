# World-Trade-Data-Analysis

#### Analysis of world wide trading. That is Country's Imports and Exports Analysis Using Hadoop MapReduce

## Commands of execution are below:

### Steps:

1)Extract World-Trade-Data-Analysis folder from zip to Desktop.

2)Use copyFromLocal to copy file (2018-2010_export.csv and 2018-2010_import.csv) from local to hdfs. 

```hadoop fs -mkdir /user/cloudera/tradedata```

```hadoop fs -copyFromLocal 2018-2010_export.csv /user/cloudera/tradedata/```

```hadoop fs -copyFromLocal 2018-2010_import.csv /user/cloudera/tradedata/```

Note:-Here these two files (2018-2010_export.csv and 2018-2010_import.csv) are 2 input files represents country's Trade goods imports and exports.

3)Give all permissions to .java files go through all inner folders.So you can find .java files. Give rwx permissions for all users.

Example:

```chmod 777 /home/cloudera/World-Trade-Data-Analysis/src/main/java/com/samhad/app/CYApp.java```

4)And also files you copied from local file system to hdfs,Give rwx permissions to 2 of those files.

```hadoop fs -chmod 777 /user/cloudera/tradedata/2018-2010_export.csv /user/cloudera/tradedata/2018-2010_import.csv```

## Now execute below final commands:

#### To analyse overall data on the basis of Country and Year

```hadoop jar worldwidetrade.jar com.samhad.app.CYApp <hdfs 2018-2010_export.csv file path> <hdfs 2018-2010_import.csv file path> <hdfs output directory path (outut will be stored here) >```

#### To analyse overall data on the basis of Commodity, Country and Year

```hadoop jar worldwidetrade.jar com.samhad.app.CYCApp <hdfs 2018-2010_export.csv file path> <hdfs 2018-2010_import.csv file path> <hdfs output directory path (outut will be stored here) >```
