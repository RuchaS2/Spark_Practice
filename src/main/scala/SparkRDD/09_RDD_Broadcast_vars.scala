package SparkRDD

import org.apache.spark.sql.SparkSession
/*
Broadcast variables -
Broadcast variables are read-only shared variables
that are cached and available on all nodes in a cluster in-order to access or use by the tasks.

Working of broadcast variables -
1. Spark breaks the job into stages that have distributed shuffling and actions are executed with in the stage.
2. Later Stages are also broken into tasks
3. Spark broadcasts the common data (reusable) needed by tasks within each stage.
4. The broadcasted data is cache in serialized format and deserialized before executing each task.
Note that broadcast variables will be sent to executors when they are first used.
 */

object RDD_Broadcast_vars {

  def Broadcast( states: Map[String, String], countries : Map[String, String], spark: SparkSession)={
    val broadcastStates = spark.sparkContext.broadcast(states)
    val broadcastCountries = spark.sparkContext.broadcast(countries)

    val data = Seq(("James","Smith","USA","CA"),
      ("Michael","Rose","USA","NY"),
      ("Robert","Williams","USA","CA"),
      ("Maria","Jones","USA","FL")
    )

    val rdd = spark.sparkContext.parallelize(data)

    val rdd2 = rdd.map(
      f=>{
        val country = f._3
        val state = f._4
        val fullcountry = broadcastCountries.value.get(country).get
        val fullstates = broadcastCountries.value.get(state).get
        (f._1 , f._2 , fullcountry , fullstates)
      }
    )
    println(rdd2.collect().mkString("\n"))
  }


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkByExamples.com")
      .master("local")
      .getOrCreate()

    val states = Map(("NY","New York"),("CA","California"),("FL","Florida"))
    val countries = Map(("USA","United States of America"),("IN","India"))

    Broadcast(states , countries, spark)
  }
}
