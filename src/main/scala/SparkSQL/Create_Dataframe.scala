package SparkSQL

import org.apache.spark.sql.SparkSession

/*
Dataframes -
distributed collection of data organized into named columns. It is conceptually equivalent to a table in a relational database
You can also create a DataFrame from different sources like Text, CSV, JSON, XML, Parquet, Avro, ORC, Binary files,
RDBMS Tables, Hive, HBase, and many more.
Ways-
1. Spark Create DataFrame from RDD - toDF() , createDataFrame() from SparkSession , createDataFrame() with the Row type

 */
object Create_Dataframe {

  val spark = SparkSession.builder().appName("Accumulators").master("local[1]").getOrCreate()
  import spark.implicits._
  val columns = Seq("language","users_count")
  val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))

  //1. Spark Create DataFrame from RDD
  val rdd = spark.sparkContext.parallelize(data)
  val dfFromRDD1 = rdd.toDF()
  dfFromRDD1.printSchema()
  val dfFromRDD2 = rdd.toDF("language","users_count") //with col names
  dfFromRDD1.printSchema()



}
