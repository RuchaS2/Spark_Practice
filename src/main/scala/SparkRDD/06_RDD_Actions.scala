package SparkRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/*
Actions -
RDD Actions
-operations that return the raw values
-any RDD function that returns other than RDD[T] is considered as an action
1.Aggregate - aggregate() the elements of each partition, and then the results for all the partitions.
2.Fold - Aggregate the elements of each partition, and then the results for all the partitions.
3.Reduce - Reduces the elements of the dataset using the specified binary operator.
4.Collect - Return the complete dataset as an Array.
5.count – Return the count of elements in the dataset.
6.countApprox() – Return approximate count of elements in the dataset, this method returns incomplete when execution time meets timeout.
7.countApproxDistinct() – Return an approximate number of distinct elements in the dataset.
8.countByValue – Return Map[T,Long] key representing each unique value in dataset and value represents count each value present.
9.countByValueApprox – Same as countByValue() but returns approximate result.
10.first() – Return the first element in the dataset.
11.top() – Return top n elements from the dataset.
12.min() – Return the minimum value from the dataset.
13.max() – Return the maximum value from the dataset.
14.take() – Return the first num elements of the dataset.
15.takeOrdered() – Return the first num (smallest) elements from the dataset
16.takeSample() – Return the subset of the dataset in an Array.

*/

object RDD_Actions {

  def actions(inputRDD: RDD[(String,Int)] , listRDD : RDD[Int])={

    //Aggregate
    def param1 = (accu:Int , v:Int) => accu + v
    def param2= (accu1:Int,accu2:Int) => accu1 + accu2
    println("Aggregate : "+ listRDD.aggregate(0)(param1 , param2))

    //Fold
    println("Fold:" + listRDD.fold(0){ (acc,v) =>
      val sum = acc + v
      sum
    })
    println("fold :  "+inputRDD.fold(("Total",0)){ (acc:(String,Int),v:(String,Int)) =>
      val sum = acc._2 + v._2
      ("Total",sum)
    })

    //Reduce
    println("reduce:" + listRDD.reduce( _ + _ ) )
    println("reduce alternate: "+ listRDD.reduce( (x,y) => x + y))
    println("reduce:"+ inputRDD.reduce((x,y) => ("Total", x._2 + y._2)))

    //collect
    val data:Array[Int] = listRDD.collect()
    data.foreach(println)

    //Count
    println("Count : "+listRDD.count)
    println("countApprox : "+listRDD.countApprox(1200))
    println("countApproxDistinct : "+listRDD.countApproxDistinct())
    println("countApproxDistinct : "+inputRDD.countApproxDistinct())
    println("countByValue :  "+listRDD.countByValue())
    println("first :  "+listRDD.first())
    println("first :  "+inputRDD.first())

    //top()
    println("top: "+ listRDD.top(2).mkString(","))
    println("top: "+ inputRDD.top(2).mkString(":"))

    //min
    println("min :  "+listRDD.min())
    println("min :  "+inputRDD.min())

    //max
    println("min :  "+listRDD.max())
    println("min :  "+inputRDD.max())

    //take
    println("take : "+listRDD.take(2).mkString(","))

  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SparkByExample")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val inputRDD = spark.sparkContext.parallelize(List(("Z", 1),("A", 20),("B", 30),("C", 40),("B", 30),("B", 60)))

    val listRdd = spark.sparkContext.parallelize(List(1,2,3,4,5,3,2))

    actions(inputRDD , listRdd)
  }
}
