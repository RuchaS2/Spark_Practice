/*
Spark RDD - Resilient Distributed Dataset
fundamental DS, immutable distributed collection of objects.
RDD is computed on several JVMs scattered across
multiple physical servers also called nodes in a cluster.

RDD Advantages -
In memory Processing ,Immutability ,Fault Tolerance
Lazy Evolution , partitioning , Parallelize
 */
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
object RDDBasics {

  /* Parallelize -
     A new distributed data set is created with specified number of partitions
     and the elements of the collection are copied to the distributed dataset
     parallelize() method acts lazy.
     public <T> RDD<T> parallelize( seq, numSlices, evidence$1 )
     */
  def RDD_parallelize() = {
    val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("RDD_parallelize")
      .getOrCreate()

    val rdd: RDD[Int] = spark.sparkContext.parallelize(List(1, 2, 3, 4, 5))
    val rddCollect: Array[Int] = rdd.collect()
    rddCollect.foreach(println)
  }

  // Create RDD
  def createRDD() = {
    val spark = SparkSession.builder().master("local[1]")
      .appName("CreateRDD")
      .getOrCreate()

    //Way 1 - Spark Create RDD from Seq or List (using Parallelize)
    val rdd1 = spark.sparkContext.parallelize(Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000)))
    rdd1.foreach(println)

    //Way 2 - Create an RDD from a text file
    val filePath="src/main/resources/textFile.txt"
    val rdd2 = spark.sparkContext.wholeTextFiles(filePath)
    rdd2.foreach(record => println("FileName : " + record._1 + ", FileContents :" + record._2))

    //Way 3 - Creating from another RDD
    val rdd3 = rdd1.map(row => {
      (row._1, row._2 + 100)
    })

    //Way 4 - From existing DataFrames and DataSet
    val myRdd2 = spark.range(20).toDF().rdd
  }

  //Create Empty RDD
  def emptyRDD(): Unit={
    val spark:SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("emptyRDD")
      .getOrCreate()

    //Empty RDD without partitions
    val rdd = spark.sparkContext.emptyRDD                //Creates EmptyRDD[0]
    val rddString = spark.sparkContext.emptyRDD[String]  //Creates EmptyRDD[1]
    println(rdd)
    println(rddString)
    println("Num of Partitions: "+rdd.getNumPartitions)

    //Empty RDD with partitions using parallelize
    val rdd2 = spark.sparkContext.parallelize(Seq.empty[String])
    println(rdd2)
    println("Num of Partitions: "+rdd2.getNumPartitions)

  }

  def main(args: Array[String]): Unit = {
    RDD_parallelize()
    createRDD()
    emptyRDD()
  }
}
