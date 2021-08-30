/*
RDD transformations -
transformations always create new RDD without updating an existing one hence, this creates an RDD lineage.
Lazy Transformation - (RDD Transformations are lazy operations)
none of the transformations get executed until you call an action on Spark RDD.

Transformation types
1. Narrow- map(), mapPartition(), flatMap(), filter(), union() functions
   these compute data that live on a single partition
   NO data movement between partitions

2. Wider (Shuffle)- groupByKey(), aggregateByKey(), aggregate(), join(), repartition()
   these compute data that live on many partitions
   data movements between partitions
 */
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RDD_operations {
  //Word count example

  //Flatmaps
  //transformation flattens the RDD after applying the function and returns a new RDD.
  def flat_maps(rdd: RDD[String]):RDD[String]={
    val rdd2 = rdd.flatMap(f=>f.split(" ")) //split by space
    return rdd2
  }

  //Maps
  //apply any complex operations like adding a column, updating a column
  //output of map transformations would always have the same number of records as input.
  def maps(rdd2:RDD[String]):RDD[String]={
    val rdd3:RDD[String] = rdd2.map(m=>(m,1))
    //Adding new column
    return rdd3
  }

  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("WordCount")
      .getOrCreate()
    val sc = spark.sparkContext

    val rdd:RDD[String] = sc.textFile("src/main/resources/wordCount.txt")

    val rdd2:RDD[String]= flat_maps(rdd)
    val rdd3:RDD[String]= maps(rdd2)

  }
}
