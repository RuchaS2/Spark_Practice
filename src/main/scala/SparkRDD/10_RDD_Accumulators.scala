package SparkRDD

import org.apache.spark.sql.SparkSession

/*
ACCUMULATORS -
Spark Accumulators are shared variables which are only “added” through an associative and commutative operation
and are used to perform counters (Similar to Map-reduce counters) or sum operations
Types - named accumulators , unnamed accumulators

Spark by default provides accumulator methods for long, double and collection types.
All these methods are present in SparkRDD.SparkContext class and return
<a href="#LongAccumulator">LongAccumulator</a>,
<a href="#DoubleAccumulator">DoubleAccumulator</a>, and
<a href="#CollectionAccumulator">CollectionAccumulator</a> respectively.

LongAccumulator, Double  accumulator methods -
 - isZero , copy, reset, add,count ,sum,avg ,merge ,value
Collection accumulator methods -
 - isZero,copyAndReset,copy,reset,add,merge,value


 */

object RDD_Accumulators {
  def accum(spark:SparkSession)={
    val longAcc = spark.sparkContext.longAccumulator("SumAccum")
    val rdd = spark.sparkContext.parallelize(Array(1,2,3))

    rdd.foreach(x => longAcc.add(x))
    println(longAcc.value)

    val doubleAcc = spark.sparkContext.doubleAccumulator("CountAccum")
    rdd.foreach(y => doubleAcc.count)
    println(doubleAcc.value)

    val collectionAccum = spark.sparkContext.collectionAccumulator("CollectAccum")
    rdd.foreach(z => collectionAccum.isZero)
    println(collectionAccum.value)
  }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Accumulators").master("local[0]").getOrCreate()
    accum(spark)
  }
}
