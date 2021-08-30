/*
Scala Context
Since Spark 1.x, Spark SparkContext is an entry point to Spark and defined in org.apache.spark package
used to programmatically create Spark RDD, accumulators, and broadcast variables on the cluster.
You can create only one SparkContext per JVM.

SparkContext are also present in SparkSession
Spark session internally creates the Spark Context.
 */
import org.apache.spark.{SparkConf, SparkContext}
object SparkContext {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("sparkContext").setMaster("local[1]")

    //Register it as a singleton object -  use this to create Spark RDD
    val sparkContext = new SparkContext(sparkConf)

    println("First SparkContext:")
    println("APP Name :" + sparkContext.appName)
    println("Deploy Mode :" + sparkContext.deployMode)
    println("Master :" + sparkContext.master)

    sparkContext.stop()

    val conf2 = new SparkConf().setAppName("sparkContext-2").setMaster("local[1]")
    val sparkContext2 = new SparkContext(conf2)

    println("APP Name :"+sparkContext2.appName);
    println("Deploy Mode :"+sparkContext2.deployMode);
    println("Master :"+sparkContext2.master);
  }
}
