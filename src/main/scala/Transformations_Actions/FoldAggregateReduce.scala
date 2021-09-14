/*
fold() function -  first aggregating elements in each partition
and then aggregating results of all partitions to get the final result.
def fold(zeroValue: T)(op: (T, T) => T): T
op – an operator used to both accumulate results within a partition and combine results from all partitions

FOLD SIMILAR TO -
 - reduce() except it takes a ‘Zero value‘ as an initial value for each partition.
 - aggregate() with a difference; fold return type should be the same as this RDD element type whereas aggregation can return any type.
 - foldByKey() except foldByKey() operates on Pair RDD
------------------------------------------------------------------------------------------------------------
Aggregate()
def aggregate[U](zeroValue: U)(seqOp: (U, T) ⇒ U, combOp: (U, U) ⇒ U)
     (implicit arg0: ClassTag[U]): U
zeroValue – Initial value to be used for each partition in aggregation,
this value would be used to initialize the accumulator. we mostly use 0 for integer and Nil for collections.
seqOp – This operator is used to accumulate the results of each partition, and stores the running accumulated result to U
combOp – This operator is used to combine the results of all partitions U.

AGGREGATE SIMILAR TO-
 -fold() and reduce() except it returns RDD type of any time was as other 2 returns same RDD type.
 -aggregateByKey() except for aggregateByKey() operates on Pair RDD
------------------------------------------------------------------------------------------------------------
REDUCE()-
RDD reduce() function takes function type as an argument and returns the RDD with the same type as input.
It reduces the elements of the input RDD using the binary operator specified.
def reduce(f: (T, T) => T): T

Reduce by key()-
merge the values of each key using an associative reduce function.
It is a wider transformation as it shuffles data across multiple partitions and it operates on pair RDD (key/value pair)
reduceByKey(func, numPartitions=None, partitionFunc=<function portable_hash>)

sortbykey()-
sortByKey(ascending:Boolean,numPartitions:int):org.apache.spark.rdd.RDD[scala.Tuple2[K, V]]
ascending is used to specify the order of the sort, by default, it is true meaning ascending order, use false for descending order.
numPartitions is used to specify the number of partitions it should create with the result of the sortByKey() function.
 */
package Transformations_Actions

import org.apache.spark.sql.SparkSession

object RDD_FoldAggregate {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Accumulators").master("local[1]").getOrCreate()

    //fold
    val listRdd = spark.sparkContext.parallelize(List(1,2,3,4,5,3,2))

    println("Total (using fold) : "+ listRdd.fold(0)((acc,ele) => {acc + ele}))
    println("Total with init value 2 : "+  listRdd.fold(2)((acc,ele) => {acc + ele}))
    println("Min : "+listRdd.fold(0)((acc,ele) => {acc min ele}))
    println("Max : "+listRdd.fold(0)((acc,ele) => {acc max ele}))

    //AGGREGATE
    def param0= (accu:Int, v:Int) => accu + v
    def param1= (accu1:Int,accu2:Int) => accu1 + accu2
    val result = listRdd.aggregate(0)(param0,param1)
    println("output 1 =>" + result)


    //reduce-
    println("output sum using binary : "+listRdd.reduce(_ min _))
    println("output min using binary : "+listRdd.reduce(_ max _))
    println("output max using binary : "+listRdd.reduce(_ + _))

    //OR
    println("output min : "+listRdd.reduce((a, b ) => a min b))
    println("output max : "+listRdd.reduce((a, b ) => a max b))
    println("output sum : "+listRdd.reduce((a, b ) => a + b))


    //ReducebyKey
    val data = Seq(("Project", 1),
      ("Gutenberg’s", 1),
      ("Alice’s", 1),
      ("Adventures", 1),
      ("in", 1),
      ("Wonderland", 1),
      ("Project", 1),
      ("Gutenberg’s", 1),
      ("Adventures", 1),
      ("in", 1),
      ("Wonderland", 1),
      ("Project", 1),
      ("Gutenberg’s", 1))

    val rdd=spark.sparkContext.parallelize(data)

    val rdd2=rdd.reduceByKey(_ + _) //counts all
    rdd2.foreach(println)

    //sortByKey -
    val data1 = Seq(("Project","A", 1),
      ("Gutenberg’s", "X",3),
      ("Alice’s", "C",5),
      ("Adventures","B", 1)
    )
    val rdd3=spark.sparkContext.parallelize(data1)
    val rdd4=rdd3.map(f=>{(f._2, (f._1,f._2,f._3))}) //adding key column
    rdd4.foreach(println)

    val rdd5= rdd4.sortByKey()
    rdd5.foreach(println)

  }

}
