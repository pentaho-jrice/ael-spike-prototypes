package com.pentaho.adaptiveexecution.prototype.nativesparkuniquerows;

import org.apache.commons.lang.time.StopWatch;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Prototype class to demonstrate the use of Spark SQL DataFrames to return Unique Rows per the requirements of the
 * PDI Unique Rows step.
 * <p>
 * This class will read in sales data from csv file, transforms it to JavaRDDs, then transoforms it into Spark SQL
 * Dataframes, then run through various unique row scenarios.
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

    // read the sample data csv in as JavaRDD and transform it into SalesDataModel RDDs
    JavaRDD<SalesDataModel> salesDataJavaRdd = jsc.textFile( "../ktrs/files/sales_data - simple.csv" ).map( line -> {
      String[] fields = line.split( "," );

      SalesDataModel sd = null;

      try {
        sd = new SalesDataModel( fields[ 0 ], fields[ 1 ], fields[ 2 ], fields[ 3 ], fields[ 4 ], fields[ 6 ], fields[ 7 ],
          fields[ 17 ], fields[ 18 ], fields[ 19 ], fields[ 20 ] );
      } catch (Exception e) {
        System.out.println("   --> Error:  error tryin to parse this line:  " + line);
        sd = new SalesDataModel();
      }

      return sd;
    } );

    // some debug code.  Uncomment to validate you got the data in as expected
    //    rdd_records.foreach( a -> System.out.println(a.getCity() + " - " + a.getState()));

    // Transform the sales data JavaRDD to Spark SQL Dataframe (i.e., Dataset<row>)
    Dataset<Row> salesDataDataFrame = spark
      .createDataFrame( salesDataJavaRdd, SalesDataModel.class )
      .sort( "city", "state" );

    // Print the sales data schema to the console.
    salesDataDataFrame.printSchema();

    // Print 100 rows of sales data Dataframe to the console.
    salesDataDataFrame.show( 100);

    // register the sales data as a table
    salesDataDataFrame.registerTempTable( "salesdata" );

    // ---------------------------------------------------------------------------------------------
    // run all the different Unique Row Scenarios against the salesdata table that was registered above.
    //
    // These are all the different scenarios supported by PDI UniqueRows Step
    // ---------------------------------------------------------------------------------------------

    executeUniqueRowsNamedColumnsOperation( spark, salesDataDataFrame, "Unique Rows with Named Columns" );

    executeUniqueRowsAllColumnsOperation( spark, salesDataDataFrame, "Unique Rows with All Columns" );

    executeUniqueRowsNamedColumnsWithDuplicateOperation( spark, salesDataDataFrame, "Unique Rows with All Columns and Duplicate Row Count" );

    executeUniqueRowsAllColumnsReturnRejectedRowsOperation( spark, salesDataDataFrame, "Unique Rows with All Columns - return rejected rows");
  }

  /**
   * Unique Rows with Named Columns
   *
   * In this scenario, you need to remove all rows where the values for 2 or more Columns are duplicated.
   *
   * Only the first row of the matching column group should be returned.
   *
   * @param spark
   * @param salesDataDataFrame
   * @param uniqueRowsScenario
   */
   private static void executeUniqueRowsNamedColumnsOperation( SparkSession spark, Dataset<Row> salesDataDataFrame, String uniqueRowsScenario ) {
    StopWatch stopWatch = logOperationStart( uniqueRowsScenario );

    // Drop the duplicate rows where city and state columns are identical
    Dataset<Row> groupDf = salesDataDataFrame.dropDuplicates( "city", "state" );

    // show the results of the query.
    groupDf.show();

    logOperationEnd( uniqueRowsScenario, stopWatch );
  }

  /**
   * Unique Rows with All Columns
   *
   * In this scenario, you need to remove all rows where the values in ALL columns match.  i.e. an exact match.
   *
   * Only the first row of the matching column group should be returned.
   *
   * @param spark
   * @param salesDataDataFrame
   * @param uniqueRowsScenario
   */
  private static void executeUniqueRowsAllColumnsOperation( SparkSession spark, Dataset<Row> salesDataDataFrame, String uniqueRowsScenario ) {
    StopWatch stopWatch = logOperationStart( uniqueRowsScenario );

    // Drop the duplicate rows where ALL columns are identical
    Dataset<Row> groupDf = salesDataDataFrame.dropDuplicates(  );

    // show the results of the query.
    groupDf.show();

    logOperationEnd( uniqueRowsScenario, stopWatch );
  }

  /**
   * Unique Rows with All Columns and Duplicate Row Count
   *
   * In this scenario, you need to remove all rows where the values for 2 or more Columns are duplicated. And add a new
   * column with the count of total duplicates found for that match.
   *
   * Only the first row of the matching column group should be returned.
   *
   * @param spark
   * @param salesDataDataFrame
   * @param uniqueRowsScenario
   */
  private static void executeUniqueRowsNamedColumnsWithDuplicateOperation( SparkSession spark, Dataset<Row> salesDataDataFrame, String uniqueRowsScenario ) {
    StopWatch stopWatch = logOperationStart( uniqueRowsScenario );

    // run Spark SQL Group By query to add a column to the DataFrame of the # of duplicate rows.
    Dataset<Row> groupDf = spark.sql(
            "SELECT A.*, " +
                    "      b.duplicateCount" +
                    " FROM salesdata A" +
                    "      LEFT OUTER JOIN ( SELECT city, " +
                    "                               state, " +
                    "                               count(*) duplicateCount" +
                    "                          FROM salesdata B" +
                    "                      GROUP BY city, state) B "
                    + "    ON  A.city = B.city AND" +
                    "          A.state = B.state" +
                    " ORDER BY 1, 2 ").sort("city", "state");

    // show the results of the query.
    groupDf.show();

    // Now that we added the duplicate count column, drop the duplicate rows where city and state columns are identical
    groupDf = groupDf.dropDuplicates( "city", "state" ).sort("city", "state");;

    // show the results of the query.
    groupDf.show();

    logOperationEnd( uniqueRowsScenario, stopWatch );
  }

  /**
   * Unique Rows with Named Columns - Return rejectedd rows
   *
   * this scenario supports a feature of the PDI Unique Rows step, where users can specify that all rejected rows be
   * redirected
   *
   * In that case, all duplicate records that are kept are sent to output stream and all duplicate records that are
   * rejected are sent to the error stream.
   *
   * So we need to have a way of getting both result sets to return.  This scenario focuses on how to get the rejects.
   *
   * @param spark
   * @param salesDataDataFrame
   * @param uniqueRowsScenario
   */
  private static void executeUniqueRowsAllColumnsReturnRejectedRowsOperation( SparkSession spark, Dataset<Row> salesDataDataFrame, String uniqueRowsScenario ) {
    StopWatch stopWatch = logOperationStart( uniqueRowsScenario );

    System.out.println( "\n   Unique Rows Scenario '" + uniqueRowsScenario + " Not Implemented Yet\n" );

//    // Drop the duplicate rows where ALL columns are identical
//    Dataset<Row> groupDf = salesDataDataFrame.dropDuplicates(  );
//
//    // show the results of the query.
//    groupDf.show();

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