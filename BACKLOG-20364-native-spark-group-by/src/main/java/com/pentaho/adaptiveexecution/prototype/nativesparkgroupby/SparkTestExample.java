package com.pentaho.adaptiveexecution.prototype.nativesparkgroupby;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class SparkTestExample {
  public static void main(String[] args) {
    String logFile = "../ktrs/files/sales_data.csv"; // Should be some file on your system
    SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
    Dataset<String> logData = spark.read().textFile(logFile).cache();

    long numAs = logData.filter(s -> s.contains("a")).count();
    long numBs = logData.filter(s -> s.contains("b")).count();

    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

    spark.stop();
  }
}