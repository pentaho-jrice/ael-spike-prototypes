SPARK_HOME=/opt/spark-2.2.0-bin-hadoop2.7
SPARK_CMD=$SPARK_HOME/bin/spark-submit

SPARK_CLASS="com.pentaho.adaptiveexecution.prototype.nativesparkgroupby.SparkGroupBySql"
SPARK_DRIVER_JAR=../../target/spark-native-groupby-8.1.0.0-SNAPSHOT.jar

time $SPARK_CMD --class $SPARK_CLASS --master local[*] $SPARK_DRIVER_JAR
