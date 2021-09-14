package SparkRDD

import org.apache.spark.sql.SparkSession
/*
repartition() is used to increase or decrease the RDD, DataFrame, Dataset partitions
coalesce() is used to only decrease the number of partitions in an efficient way.

Spark repartition() and coalesce() are very expensive operations
coalesce is optimized or improved version of repartition()
where the movement of the data across the partitions is lower using coalesce.

Shuffle Partition - (expensive ops)
Shuffling is a mechanism Spark uses to redistribute the data across different executors
and even across machines. Spark shuffling triggers for transformation operations l
ike gropByKey(), reducebyKey(), join(), union(), groupBy() e.t.c

Spark RDD triggers shuffle for several operations
like repartition(), coalesce(),  groupByKey(),  reduceByKey(), cogroup() and join() but not countByKey() .

Dataframe shuffle
Unlike RDD, Spark SQL DataFrame API increases the partitions when the transformation operation performs shuffling.
DataFrame operations that trigger shufflings are join(), union() and all aggregate functions.

spark.sql.shuffle.partitions -> default 200
*/

object RDD_Storage {
  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession.builder()
      .master("local[5]")
      .appName("SparkRDD.RDD_Storage")
      .getOrCreate()

    //PARTITIONING ON RDDS ------------
    val rdd = spark.sparkContext.parallelize(Range(0,20))
    println("From local[5]"+rdd.partitions.size)

    val rdd1 = spark.sparkContext.parallelize(Range(0,25), 6) //distributes to 6 partitions
    println("parallelize : "+rdd1.partitions.size)

    val rddFromFile = spark.sparkContext.textFile("src/main/resources/wordCount.txt",10)
    println("TextFile : "+rddFromFile.partitions.size)

    //repartition
    val rdd2 = rdd1.repartition(4)
    println("Repartition size : "+rdd2.partitions.size)

    //coalesce
    val rdd3 = rdd1.coalesce(4)
    println("Repartition size : "+rdd3.partitions.size)

    //ON DATAFRAME
    val df = spark.range(0,20)
    println(df.rdd.partitions.length)

    val df2 = df.repartition(6)
    println(df2.rdd.partitions.length)

    val df3 = df.coalesce(2)
    println(df3.rdd.partitions.length)


  }
}
