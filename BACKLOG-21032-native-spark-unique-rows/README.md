



# Spark Group By Prototype

This prototype is to prove out the Unique Rows capabilities of Spark and how it can be used to create a Pentaho Unique Rows Step using Native Spark features.
* **Spike Jira** - http://jira.pentaho.com/browse/BACKLOG-21032
* **Spike Wiki Doc** - http://iwiki.pentaho.com/display/ENG/Spike+-+BACKLOG-21032+-+Native+Spark+Unique+Rows

## Prototype Goals
* Prove out the capabilities of Spark to return unique rows
* Working code using Spark SQL Dataframes API

## Main Folders

* **resources** - contains files used in the prototype
  - **ktrs** - contains sample transformations and test data used in prototype
  - **scripts**  - contains scripts to run the prototype
 - **src** - Source code
 
## Main Prototype Classes
* [SparkUniqueRowsSql](src/main/java/com/pentaho/adaptiveexecution/prototype/nativesparkuniquerows/SparkUniqueRowsSql.java)

## Running the Prototype
* Prequisites:
  - Spark SQL 2.1.0 or highr installed on the same machine you that you clone this repo on.
   
* Easiest way to run protytpe is clone the repo, cd to the scripts directory and run the scripts:
  - **build-spark-unique-row.sh** - builds the prototype .jar
  - **submit-spark-uniqe-row.sh** - submits the prototype app to spark
  - **build-submit-spark-unique-row.sh** - builds the .jar, then submits to spark.

Note:  you may have to modify the **submit-spark-uniqe-row.sh** script to point to your local version of spark.  But everything else is relative and should not need to change.