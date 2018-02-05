package com.pentaho.adaptiveexecution.prototype.nativesparkuniquerows;

import org.apache.commons.lang.time.StopWatch;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Prototype class to demonstrate the use of Spark SQL DataFrames to do Unique Rows (de-duplication) operations.
 * <p>
 * This class will read in sales data from csv file, transforms it to JavaRDDs, then transoforms it into Spark SQL
 * Dataframes, then perform unique rows operations and shows results.
 *
 * Joe Rice 2/3/2018
 */
public class SparkUniqueRowsSql {
  public static void main( String[] args ) {
    // Get a Spark Session - Spark Session is the main entrypoint for all Spark SQL Operations
    SparkSession spark = SparkSession
      .builder()
      .appName( "SparkGroupBySql" )
      .getOrCreate();

    // Get the SparkContext object from the Spark Session and convert it into a JavaSparkContext object.
    SparkContext sc = spark.sparkContext();
    JavaSparkContext jsc = JavaSparkContext.fromSparkContext( sc );

    // read the sample data csv in as JavaRDD
    JavaRDD<String> data = jsc.textFile( "../ktrs/files/sales_data.csv" );

    // Transform the csv data RDD into SalesDataModel RDDs
    JavaRDD<SalesDataModel> salesDataJavaRdd = jsc.textFile( "../ktrs/files/sales_data.csv" ).map( line -> {
      String[] fields = line.split( "," );

      SalesDataModel sd =
        new SalesDataModel( fields[ 0 ], fields[ 1 ], fields[ 2 ], fields[ 3 ], fields[ 4 ], fields[ 6 ], fields[ 7 ],
          fields[ 17 ], fields[ 18 ], fields[ 19 ], fields[ 20 ] );

      return sd;
    } );

    // some debug code.  Uncomment to validate you got the data in as expected
    //    rdd_records.foreach( a -> System.out.println(a.getCity() + " - " + a.getState()));

    // Transform the sales data JavaRDD to Spark SQL Dataframe (Dataset)
    Dataset<Row> salesDataDataFrame = spark
      .createDataFrame( salesDataJavaRdd, SalesDataModel.class )
      .sort( "city", "state" );

    // Show the schema of the sales data Dataframe - validate it is working.  Should see the schema definition and
    // sample rows from the csv
    salesDataDataFrame.show();

    // register the sales data as a table
    salesDataDataFrame.registerTempTable( "salesdata" );

    // ---------------------------------------------------------------------------------------------
    // run all the different Unique Row Scenarios against the salesdata table that was registered above.
    //
    // These are all the different scenarios supported by PDI UniqueRows Step
    // ---------------------------------------------------------------------------------------------

    executeUniqueRowsNamedColumnsOperation( spark, salesDataDataFrame, "Unique Rows with Named Columns" );

    executeUniqueRowsAllColumnsOperation( spark, salesDataDataFrame, "Unique Rows with All Columns" );

    executeUniqueRowsNamedColumnsOperation( spark, salesDataDataFrame, "Unique Rows with All Columns" );
  }

  private static void executeUniqueRowsNamedColumnsOperation( SparkSession spark, Dataset<Row> salesDataDataFrame, String uniqueRowsScenario ) {
    StopWatch stopWatch = logOperationStart( uniqueRowsScenario );

    // run Spark SQL Group By query against the sales data table
    Dataset<Row> groupDf = salesDataDataFrame.dropDuplicates( "city", "state" );

    // show the results of the query.
    groupDf.show();

    logOperationEnd( uniqueRowsScenario, stopWatch );
  }

  private static void executeUniqueRowsAllColumnsOperation( SparkSession spark, Dataset<Row> salesDataDataFrame, String uniqueRowsScenario ) {
    StopWatch stopWatch = logOperationStart( uniqueRowsScenario );

    // run Spark SQL Group By query against the sales data table
    Dataset<Row> groupDf = salesDataDataFrame.dropDuplicates(  );

    // show the results of the query.
    groupDf.show();

    logOperationEnd( uniqueRowsScenario, stopWatch );
  }

  private static StopWatch logOperationStart( String uniqueRowsScenario ) {
    StopWatch stopWatch = new StopWatch();

    System.out.println( "\n\n" );
    System.out.println( "-------------------------------------------------------------------------------------------" );
    System.out.println( "  Unique Rows Operation START..." );
    System.out.println( "     - Uique Rows Scenario:     " + uniqueRowsScenario );
    System.out.println( "-------------------------------------------------------------------------------------------" );

    stopWatch.start();

    return stopWatch;
  }

  private static void logOperationEnd( String uniqueRowsScenario, StopWatch stopWatch ) {
    stopWatch.stop();

    System.out.println( "-------------------------------------------------------------------------------------------" );
    System.out.println( "  Unique Rows Operation END..." );
    System.out.println( "     - Unique Rows Scenario:    " + uniqueRowsScenario );
    System.out.println( "     - Total Elapsed Time:  " + stopWatch.toString() );
    System.out.println( "-------------------------------------------------------------------------------------------" );
    System.out.println( "\n\n" );
  }
}