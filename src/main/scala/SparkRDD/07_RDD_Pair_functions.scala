package SparkRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/*
Pair functions
Spark defines PairRDDFunctions class with several functions to work with Pair RDD or RDD key-value pair
Spark Pair RDD Transformation Functions
	1. aggregateByKey - 	Aggregate the values of each key in a data set.
	  This function can return a different result type then the values in input RDD.
	2. combineByKey	- Combines the elements for each key.
	3. combineByKeyWithClassTag -	Combines the elements for each key.
	4. flatMapValues-	It's flatten the values of each key with out changing key values and keeps the original RDD partition.
	5. foldByKey	 -Merges the values of each key.
	6. groupByKey	 -Returns the grouped RDD by grouping the values of each key.
	7. mapValues	 -It applied a map function for each value in a pair RDD with out changing keys.
	8. reduceByKey	 -Returns a merged RDD by merging the values of each key.
	9. reduceByKeyLocally -	Returns a merged RDD by merging the values of each key and final result will be sent to the master.
	10. sampleByKey	 -Returns the subset of the RDD.
	11. subtractByKey -	Return an RDD with the pairs from this whose keys are not in other.
	12. keys	 -Returns all keys of this RDD as a RDD[T].
	13. Values -	Returns an RDD with just values.
	14. partitionBy	- Returns a new RDD after applying specified partitioner.
	15. fullOuterJoin	 -Return RDD after applying fullOuterJoin on current and parameter RDD
	16. join	 -Return RDD after applying join on current and parameter RDD
	17. leftOuterJoin	 -Return RDD after applying leftOuterJoin on current and parameter RDD
  18. rightOuterJoin -	Return RDD after applying rightOuterJoin on current and parameter RDD
 */

object RDD_Pair_functions {
  def Pair_fns(pairRDD: RDD[(String,Int)])={

    pairRDD.distinct().foreach(println)

    pairRDD.sortByKey().foreach(println)
    println("Reduce by Key ==>")
    val wordCount = pairRDD.reduceByKey((a,b)=>a+b)
    wordCount.foreach(println)

    //Aggregate by Key
    def param1= (accu:Int,v:Int) => accu + v
    def param2= (accu1:Int,accu2:Int) => accu1 + accu2
    println("Aggregate by Key ==> wordcount")

    val wordCount2 = pairRDD.aggregateByKey(0)(param1,param2)
    wordCount2.foreach(println)

    //Keys
    println("Keys ==>")
    wordCount2.keys.foreach(println)
    //values
    println("Values ==>")
    wordCount2.values.foreach(println)

    //Count
    println("Count :"+wordCount2.count())

    //Collect as Map
    println("collectAsMap ==>")
    pairRDD.collectAsMap().foreach(println)

  }
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SparkPairFunction")
      .master("local")
      .getOrCreate()
    val rdd = spark.sparkContext.parallelize(
      List("Germany India USA","USA India Russia","India Brazil Canada China")
    )
    val wordsRdd = rdd.flatMap(_.split(" "))
    wordsRdd.foreach(println)

    val pairRDD = wordsRdd.map(f=>(f,1))
    pairRDD.foreach(println)

    Pair_fns(pairRDD)
  }
}
