package com.pentaho.adaptiveexecution.prototype.nativesparkgroupby;

import org.apache.commons.lang.time.StopWatch;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Prototype class to demonstrate the use of Spark SQL DataFrames and Select Group By queries to do Group By
 * aggregations against data.
 * <p>
 * This class will read in sales data from csv file, transforms it to JavaRDDs, then transoforms it into Spark SQL
 * Dataframes, then run Select Group By SQL queries against the data and shows results.
 * <p>
 * Joe Rice 2/3/2018
 */
public class SparkGroupBySql {
    public static void main(String[] args) {
        // Get a Spark Session - Spark Session is the main entrypoint for all Spark SQL Operations
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkGroupBySql")
                .getOrCreate();

        // Get the SparkContext object from the Spark Session and convert it into a JavaSparkContext object.
        SparkContext sc = spark.sparkContext();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);

        // read the sample data csv in as JavaRDD and transform it into SalesDataModel RDDs
        JavaRDD<SalesDataModel> salesDataJavaRdd = jsc.textFile("../ktrs/files/sales_data - simple.csv").map(line -> {
            String[] fields = line.split(",");

            SalesDataModel sd = null;

            try {
                sd = new SalesDataModel(fields[0], fields[1], fields[2], fields[3], fields[4], fields[6], fields[7],
                        fields[17], fields[18], fields[19], fields[20]);
            } catch (Exception e) {
                System.out.println("   --> Error:  error tryin to parse this line:  " + line);
                sd = new SalesDataModel();
            }

            return sd;
        });


        // some debug code.  Uncomment to validate you got the data in as expected
        //    rdd_records.foreach( a -> System.out.println(a.getCity() + " - " + a.getState()));

        // Transform the sales data JavaRDD to Spark SQL Dataframe (i.e., Dataset<row>)
        Dataset<Row> salesDataDataFrame = spark
                .createDataFrame(salesDataJavaRdd, SalesDataModel.class)
                .sort("city", "state");

        // Print the sales data schema to the console.
        salesDataDataFrame.printSchema();

        // Print 100 rows of sales data Dataframe to the console.
        salesDataDataFrame.show(100);

        // register the sales data as a table
        salesDataDataFrame.registerTempTable("salesdata");

        // ---------------------------------------------------------------------------------------------
        // run all the different GroupBy aggregations against the salesdata table that was registered above.
        //
        // These are all 17 Group By Aggregates supported by PDI GfroupBy Step
        // ---------------------------------------------------------------------------------------------

        executeSumOperation(spark, "Sum");

        executeSumOperationAllRows(spark, "Sum - All Rows");

        executeAverageOperation(spark, "Average (Mean)");

        executeMedianOperation(spark, "Median");

        executePercentileOperation(spark, "Percentile");

        executeMinimumOperation(spark, "Minimum");

        executeMaximumOperation(spark, "Maximum");

        executeNumberOfValuesOperation(spark, "Number of values (N)");

        executeConcatenateStringsCommaOperation(spark, "Concatenate strings separated by , (comma)");

        executeFirstNonNullValueOperation(spark, "First non-null value");

        executeLastNonNullValueOperation(spark, "Last non-null value");

        executeFirstValueIncludingNullValueOperation(spark, "First value (including null)");

        executeLastValueIncludingNullValueOperation(spark, "Last value (including null)");

        executeCumulativeSumOperation(spark, "Cumulative sum (all rows only)");

        executeCumulativeAverageOperation(spark, "Cumulative average (all rows only)");

        executeStandardDeviationOperation(spark, "Standard Deviation");

        executeConcatenateStringsUserDelimeOperation(spark, "Concatenate strings separated by user defined delimeter");

        executeNumberOfDistinctValuesOperation(spark, "Number of distinct values");
    }

    private static void executeSumOperation(SparkSession spark, String aggregationType) {
        StopWatch stopWatch = logOperationStart(aggregationType);

        // run Spark SQL Group By query against the sales data table
        Dataset<Row> groupDf = spark.sql(
                "SELECT city, state, sum(priceEach)"
                        + " FROM salesdata"
                        + " group by city, state");

        // show the results of the query.
        groupDf.show();

        logOperationEnd(aggregationType, stopWatch);
    }

    private static void executeSumOperationAllRows(SparkSession spark, String aggregationType) {
        StopWatch stopWatch = logOperationStart(aggregationType);

        // run Spark SQL Group By query against the sales data table
        Dataset<Row> groupDf = spark.sql(
                "SELECT A.*, " +
                        "      B.sumPrice" +
                        " FROM salesdata A" +
                        "      LEFT OUTER JOIN ( SELECT city, " +
                        "                               state, " +
                        "                               sum(priceEach) sumPrice" +
                        "                          FROM salesdata B" +
                        "                      GROUP BY city, state) B "
                        + "    ON  A.city = B.city AND" +
                        "          A.state = B.state" +
                        " ORDER BY 1, 2 ");

        // show the results of the query.
        groupDf.show();

        logOperationEnd(aggregationType, stopWatch);
    }

    private static void executeAverageOperation(SparkSession spark, String aggregationType) {
        StopWatch stopWatch = logOperationStart(aggregationType);

        // run Spark SQL Group By query against the sales data table
        Dataset<Row> groupDf = spark.sql(
                "SELECT city, state, avg(priceEach) "
                        + " FROM salesdata"
                        + " group by city, state");

        // show the results of the query.
        groupDf.show();

        logOperationEnd(aggregationType, stopWatch);
    }

    private static void executeMedianOperation(SparkSession spark, String aggregationType) {
        StopWatch stopWatch = logOperationStart(aggregationType);

        // run Spark SQL Group By query against the sales data table
        Dataset<Row> groupDf = spark.sql(
                "SELECT city, state, percentile_approx(priceEach, 0.5) "
                        + " FROM salesdata"
                        + " group by city, state");

        // show the results of the query.
        groupDf.show();

        logOperationEnd(aggregationType, stopWatch);
    }

    private static void executePercentileOperation(SparkSession spark, String aggregationType) {
        StopWatch stopWatch = logOperationStart(aggregationType);

        // 20th percentile
        double percentileVal = 0.2;

        // run Spark SQL Group By query against the sales data table
        Dataset<Row> groupDf = spark.sql(
                "SELECT city, state, percentile_approx(priceEach, " + percentileVal + ") "
                        + " FROM salesdata"
                        + " group by city, state");

        // show the results of the query.
        groupDf.show();

        logOperationEnd(aggregationType, stopWatch);
    }

    private static void executeMinimumOperation(SparkSession spark, String aggregationType) {
        StopWatch stopWatch = logOperationStart(aggregationType);

        // run Spark SQL Group By query against the sales data table
        Dataset<Row> groupDf = spark.sql(
                "SELECT city, state, min(priceEach) "
                        + " FROM salesdata"
                        + " group by city, state");

        // show the results of the query.
        groupDf.show();

        logOperationEnd(aggregationType, stopWatch);
    }

    private static void executeMaximumOperation(SparkSession spark, String aggregationType) {
        StopWatch stopWatch = logOperationStart(aggregationType);

        // run Spark SQL Group By query against the sales data table
        Dataset<Row> groupDf = spark.sql(
                "SELECT city, state, max(priceEach) "
                        + " FROM salesdata"
                        + " group by city, state");

        // show the results of the query.
        groupDf.show();

        logOperationEnd(aggregationType, stopWatch);
    }

    private static void executeNumberOfValuesOperation(SparkSession spark, String aggregationType) {
        StopWatch stopWatch = logOperationStart(aggregationType);

        // run Spark SQL Group By query against the sales data table
        Dataset<Row> groupDf = spark.sql(
                "SELECT city, state, count(priceEach) "
                        + " FROM salesdata"
                        + " group by city, state");

        // show the results of the query.
        groupDf.show();

        logOperationEnd(aggregationType, stopWatch);
    }

    private static void executeConcatenateStringsCommaOperation(SparkSession spark, String aggregationType) {
        StopWatch stopWatch = logOperationStart(aggregationType);

        System.out.println("\n   Aggregation Type '" + aggregationType + " Not Implemented Yet\n");

        //    // run Spark SQL Group By query against the sales data table
        //    Dataset<Row> groupDf = spark.sql(
        //      "SELECT city, state, sum(priceEach), avg(priceEach), stddev(priceEach) "
        //        + " FROM salesdata"
        //        + " group by city, state" );
        //
        //    // show the results of the query.
        //    groupDf.show();

        logOperationEnd(aggregationType, stopWatch);
    }

    private static void executeFirstNonNullValueOperation(SparkSession spark, String aggregationType) {
        StopWatch stopWatch = logOperationStart(aggregationType);

        System.out.println("\n   Aggregation Type '" + aggregationType + " Not Implemented Yet\n");

        //    // run Spark SQL Group By query against the sales data table
        //    Dataset<Row> groupDf = spark.sql(
        //      "SELECT city, state, sum(priceEach), avg(priceEach), stddev(priceEach) "
        //        + " FROM salesdata"
        //        + " group by city, state" );
        //
        //    // show the results of the query.
        //    groupDf.show();

        logOperationEnd(aggregationType, stopWatch);
    }

    private static void executeLastNonNullValueOperation(SparkSession spark, String aggregationType) {
        StopWatch stopWatch = logOperationStart(aggregationType);

        System.out.println("\n   Aggregation Type '" + aggregationType + " Not Implemented Yet\n");

        //    // run Spark SQL Group By query against the sales data table
        //    Dataset<Row> groupDf = spark.sql(
        //      "SELECT city, state, sum(priceEach), avg(priceEach), stddev(priceEach) "
        //        + " FROM salesdata"
        //        + " group by city, state" );
        //
        //    // show the results of the query.
        //    groupDf.show();

        logOperationEnd(aggregationType, stopWatch);
    }

    private static void executeFirstValueIncludingNullValueOperation(SparkSession spark, String aggregationType) {
        StopWatch stopWatch = logOperationStart(aggregationType);

        System.out.println("\n   Aggregation Type '" + aggregationType + " Not Implemented Yet\n");

        //    // run Spark SQL Group By query against the sales data table
        //    Dataset<Row> groupDf = spark.sql(
        //      "SELECT city, state, sum(priceEach), avg(priceEach), stddev(priceEach) "
        //        + " FROM salesdata"
        //        + " group by city, state" );
        //
        //    // show the results of the query.
        //    groupDf.show();

        logOperationEnd(aggregationType, stopWatch);
    }

    private static void executeLastValueIncludingNullValueOperation(SparkSession spark, String aggregationType) {
        StopWatch stopWatch = logOperationStart(aggregationType);

        System.out.println("\n   Aggregation Type '" + aggregationType + " Not Implemented Yet\n");

        //    // run Spark SQL Group By query against the sales data table
        //    Dataset<Row> groupDf = spark.sql(
        //      "SELECT city, state, sum(priceEach), avg(priceEach), stddev(priceEach) "
        //        + " FROM salesdata"
        //        + " group by city, state" );
        //
        //    // show the results of the query.
        //    groupDf.show();

        logOperationEnd(aggregationType, stopWatch);
    }

    private static void executeCumulativeSumOperation(SparkSession spark, String aggregationType) {
        StopWatch stopWatch = logOperationStart(aggregationType);

        System.out.println("\n   Aggregation Type '" + aggregationType + " Not Implemented Yet\n");

        //    // run Spark SQL Group By query against the sales data table
        //    Dataset<Row> groupDf = spark.sql(
        //      "SELECT city, state, sum(priceEach), avg(priceEach), stddev(priceEach) "
        //        + " FROM salesdata"
        //        + " group by city, state" );
        //
        //    // show the results of the query.
        //    groupDf.show();

        logOperationEnd(aggregationType, stopWatch);
    }

    private static void executeCumulativeAverageOperation(SparkSession spark, String aggregationType) {
        StopWatch stopWatch = logOperationStart(aggregationType);

        System.out.println("\n   Aggregation Type '" + aggregationType + " Not Implemented Yet\n");

        //    // run Spark SQL Group By query against the sales data table
        //    Dataset<Row> groupDf = spark.sql(
        //      "SELECT city, state, sum(priceEach), avg(priceEach), stddev(priceEach) "
        //        + " FROM salesdata"
        //        + " group by city, state" );
        //
        //    // show the results of the query.
        //    groupDf.show();

        logOperationEnd(aggregationType, stopWatch);
    }

    private static void executeStandardDeviationOperation(SparkSession spark, String aggregationType) {
        StopWatch stopWatch = logOperationStart(aggregationType);

        // run Spark SQL Group By query against the sales data table
        Dataset<Row> groupDf = spark.sql(
                "SELECT city, state, stddev(priceEach) "
                        + " FROM salesdata"
                        + " group by city, state");

        // show the results of the query.
        groupDf.show();

        logOperationEnd(aggregationType, stopWatch);
    }

    private static void executeConcatenateStringsUserDelimeOperation(SparkSession spark, String aggregationType) {
        StopWatch stopWatch = logOperationStart(aggregationType);

        System.out.println("\n   Aggregation Type '" + aggregationType + " Not Implemented Yet\n");

        //    // run Spark SQL Group By query against the sales data table
        //    Dataset<Row> groupDf = spark.sql(
        //      "SELECT city, state, sum(priceEach), avg(priceEach), stddev(priceEach) "
        //        + " FROM salesdata"
        //        + " group by city, state" );
        //
        //    // show the results of the query.
        //    groupDf.show();

        logOperationEnd(aggregationType, stopWatch);
    }

    private static void executeNumberOfDistinctValuesOperation(SparkSession spark, String aggregationType) {
        StopWatch stopWatch = logOperationStart(aggregationType);

        // run Spark SQL Group By query against the sales data table
        Dataset<Row> groupDf = spark.sql(
                "SELECT city, state, count(distinct priceEach) "
                        + " FROM salesdata"
                        + " group by city, state");

        // show the results of the query.
        groupDf.show();

        logOperationEnd(aggregationType, stopWatch);
    }

    private static StopWatch logOperationStart(String aggregationType) {
        StopWatch stopWatch = new StopWatch();

        System.out.println("\n\n");
        System.out.println("-------------------------------------------------------------------------------------------");
        System.out.println("  GroupBy Aggregation Operation START...");
        System.out.println("     - Aggregation Type:     " + aggregationType);
        System.out.println("-------------------------------------------------------------------------------------------");

        stopWatch.start();

        return stopWatch;
    }

    private static void logOperationEnd(String aggregationType, StopWatch stopWatch) {
        stopWatch.stop();

        System.out.println("-------------------------------------------------------------------------------------------");
        System.out.println("  GroupBy Aggregation Operation END...");
        System.out.println("     - Aggregation Type:    " + aggregationType);
        System.out.println("     - Total Elapsed Time:  " + stopWatch.toString());
        System.out.println("-------------------------------------------------------------------------------------------");
        System.out.println("\n\n");
    }
}