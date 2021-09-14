package SparkRDD

import org.apache.spark.sql.SparkSession

/*
dataset -
A Dataset is a distributed collection of data.
Dataset is a new interface added in Spark 1.6
that provides the benefits of RDDs (strong typing, ability to use powerful lambda functions)
with the benefits of Spark SQL’s optimized execution engine.

dataframes -
A DataFrame is a Dataset organized into named columns.
It is conceptually equivalent to a table in a relational database or a data frame in R/Python,
but with richer optimizations under the hood.

Ways to convert RDD TO dataframes
1.using toDF()
2.using createDataFrame()
3.using RDD row type & schema

RDD TO DATASET - createDataset()
 */

object RDDtoDataframe {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Accumulators").master("local[1]").getOrCreate()

    val columns = Seq("language","users_count")
    val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
    val rdd = spark.sparkContext.parallelize(data)

    import spark.implicits._

    //1. ToDf()
    val dfFromRDD = rdd.toDF()
    dfFromRDD.printSchema()
    val dfFromRDD1 = rdd.toDF("language","users_count")
    dfFromRDD1.printSchema()

    //using createDataframe()
//    val schema = StructType(
//      columns.map(
//        fieldname => StructField(fieldname , nullable = true)
//      )
//    )

  }
}
