package com.pentaho.adaptiveexecution.prototype.nativesparkgroupby;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.vectorized.ColumnarBatch;

/**
 * Prototype class to demonstrate the use of Spark SQL DataFrames and Select Group By queries to do Group By aggregations
 * against data.
 *
 * This class will read in sales data from csv file, transforms it to JavaRDDs, then transoforms it into Spark SQL Dataframes,
 * then run Select Group By SQL queries against the data and shows results.
 */
public class SparkGroupBySql {
  public static void main(String[] args) {
    // Get a Spark Session - Spark Session is the main entrypoint for all Spark SQL Operations
    SparkSession spark = SparkSession
      .builder()
      .appName("SparkGroupBySql")
      .getOrCreate();

    // Get the SparkContext object and convert it into a JavaSparkContext object.
    SparkContext sc = spark.sparkContext();
    JavaSparkContext jsc = JavaSparkContext.fromSparkContext( sc );

    // read the sample data csv in as JavaRDD
    JavaRDD<String> data = jsc.textFile("../ktrs/files/sales_data.csv");

    // Transform the csv data RDD into SalesDataModel RDDs
    JavaRDD<SalesDataModel> rdd_records = jsc.textFile("../ktrs/files/sales_data.csv").map( line -> {
          // Here you can use JSON
          // Gson gson = new Gson();
          // gson.fromJson(line, Record.class);
          String[] fields = line.split(",");
          SalesDataModel sd = new SalesDataModel(fields[0], fields[1], fields[2], fields[3], fields[4], fields[6], fields[7], fields[17], fields[18], fields[19], fields[20]);

          return sd;
        });

    // some debug code.  Uncomment to validate you got the data in as expected
//    rdd_records.foreach( a -> System.out.println(a.getCity() + " - " + a.getState()));

    // Convert the sales data JavaRDD to Spark SQL Dataframe (Dataset)
    Dataset<Row> df = spark.createDataFrame( rdd_records, SalesDataModel.class );

    // Show the schema of the sales data Dataframe - validate it is working.  Should see the schema definition and sample rows
    // from the csv
    df.show();

    // register the sales data as atable
    df.registerTempTable( "salesdata" );

    // run Spark SQL Group By query against the sales data table
    Dataset<Row> groupDf = spark.sql(
      "SELECT city, state, sum(priceEach), avg(priceEach), stddev(priceEach) "
        + " FROM salesdata"
        + " group by city, state"
        + " order by city, state");

    // show the results of the query.
    groupDf.show();

  }
}