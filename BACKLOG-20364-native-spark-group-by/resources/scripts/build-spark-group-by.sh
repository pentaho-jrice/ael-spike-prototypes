PROJECT_HOME=../..

cd $PROJECT_HOME

time mvn clean package


#/home/devuser/ael/spark-2.2.0-bin-hadoop2.7/bin/spark-submit --class "com.pentaho.adaptiveexecution.prototype.nativesparkgroupby.SparkGroupBySql" --master local[*] spark-native-groupby-8.1.0.0-SNAPSHOT.jar