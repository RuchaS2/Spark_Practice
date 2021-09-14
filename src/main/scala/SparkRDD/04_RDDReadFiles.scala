package SparkRDD

/*
Spark – Read multiple text files into single RDD -
textFile() – Read single or multiple text, csv files and returns a single Spark RDD [String]
wholeTextFiles() – Reads single or multiple files and returns a single RDD[Tuple2[filename:String, contents:String]],

Spark Load CSV File into RDD -
Read CSV Files :
use map() transformation on RDD where we will convert RDD[String] to RDD[Array[String]
by splitting every record by comma delimiter.
map() method returns a new RDD instead of updating existing.
 */

import org.apache.spark.sql.SparkSession

object RDDReadFiles {
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkReadCSV")
    .getOrCreate()

  def readCSV()={
    val rddFromFile = spark.sparkContext.textFile("src/main/resources/text01.csv")

    //Every record in a CSV to split by comma delimiter and store it in RDD as multiple columns
    val rdd = rddFromFile.map( f => {f.split(",")} )

    //gives output along with header lines
    //Skip Header From CSV file using mapPartitionWithIndex
    rdd.foreach(f=>{
      println("Col1:"+f(0)+",Col2:"+f(1))
    })

    //using collect() -  gives Data Frames
    rdd.collect().foreach(f=>{
      println("Col1:"+f(0)+",Col2:"+f(1))
    })

    //read multiple files
    val rdd4 = spark.sparkContext.textFile("src/main/resources/text01.csv,src/main/resources/text02.csv")
    rdd4.foreach(f=>{
      println(f)
    })

  }
}
