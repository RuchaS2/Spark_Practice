package SparkRDD

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


  def transformations(rdd: RDD[String])= {

    //Flatmaps
    //transformation flattens the RDD after applying the function and returns a new RDD.
    val rdd2 = rdd.flatMap(f => f.split(" ")) //split by space

    // Maps
    //apply any complex operations like adding a column, updating a column
    //output of map transformations would always have the same number of records as input.
    //Adding new column with value 1 for each word
    val rdd3:RDD[(String,Int)]= rdd2.map(m=>(m,1))

    //Filter - words starting with a
    val rdd4 = rdd3.filter(a => a._1.startsWith("a"))

    //reduceBy key
    //reduceByKey() merges the values for each key with the function specified
    val rdd5 = rdd3.reduceByKey(_ + _)

    //SortByKey
    //first covert RDD[String,int] to RDD[int,string] using Map nd then sort
    val rdd6 = rdd5.map(a=>(a._2,a._1)).sortByKey()

    //Print rdd6 result to console
    println("Final Result")
    rdd6.foreach(println)

  }


  def main(args: Array[String]): Unit = {

    //Transformations
    val spark:SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("RDDops")
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd:RDD[String] = sc.textFile("src/main/resources/wordCount.txt")

    transformations(rdd)


  }
}
