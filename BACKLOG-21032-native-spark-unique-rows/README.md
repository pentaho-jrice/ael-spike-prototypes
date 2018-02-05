



# Spark Group By Prototype

This prototype is to prove out the Group By capabilities of Spark and how it can be used to create a Pentaho Group By Step using Native Spark features.
* **Spike Jira** - http://jira.pentaho.com/browse/BACKLOG-20364
* **Spike Wiki Doc** - http://iwiki.pentaho.com/display/ENG/Spike+-+BACKLOG-20364+-+Native+Spark+GroupBy

## Prototype Goals
* Prove out the Group By aggregation capabilities of Spark
* Working code using Spark SQL Dataframes and "Select Group By" queries

## Main Folders

* **resources** - contains files used in the prototype
  - **ktrs** - contains sample transformations and test data used in prototype
  - **scripts**  - contains scripts to run the prototype
 - **src** - Source code
 
## Main Prototype Classes
* [SparkGroupBySql](src/main/java/com/pentaho/adaptiveexecution/prototype/nativesparkgroupby/SparkGroupBySql.java)

## Running the Prototype
* Prequisites:
  - Spark SQL 2.1.0 or highr installed on the same machine you that you clone this repo on.
   
* Easiest way to run protytpe is clone the repo, cd to the scripts directory and run the scripts:
  - **build-spark-group-by.sh** - builds the prototype .jar
  - **submit-spark-group-by.sh** - submits the prototype app to spark
  - **build-submit-park-group-by.sh** - builds the .jar, then submits to spark.

Note:  you may have to modify the **submit-spark-group-by.sh** script to point to your local version of spark.  But everything else is relative and should not need to change.