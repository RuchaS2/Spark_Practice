/*
Spark Session -
entry point to underlying Spark functionality in order
to programmatically create Spark RDD, DataFrame and DataSet.

Spark Session also includes all the APIs available in different contexts –
Spark Context,SQL Context,Streaming Context,Hive Context.

Multiple sessions -
Spark gives a straight forward API to create a new session which shares the same spark context.
*/

import org.apache.spark.sql.SparkSession

object SparkTest {
  def main(args:Array[String])={

    // SparkSession will be created using SparkSession.builder() builder patterns.
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkSession")
      .getOrCreate();
    println(spark.version)

    //Accessing all contexts
    println(spark.sparkContext)
    println(spark.sqlContext)

  }

}