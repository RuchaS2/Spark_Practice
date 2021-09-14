package Transformations_Actions

import org.apache.kerby.util.Util
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._

/*
Maps
used to apply the transformation on every element of RDD, DataFrame, and Dataset and finally returns a new RDD/Dataset respectively.
Transformations like adding a column, updating a column e.t.c can be done using map,
the output of map transformations would always have the same number of records as input.

FlatMaps
Spark flatMap() transformation flattens the RDD/DataFrame column
after applying the function on every element and returns a new RDD/DataFrame respectively.
The returned RDD/DataFrame can have the same count or more number of elements.
 */
object MapFlatMaps {

  def MapOnRDD(rdd:RDD[String]): Unit ={

    //Syntax
    /*
    map[U](f : scala.Function1[T, U])(implicit evidence$3 : scala.reflect.ClassTag[U]) : org.apache.spark.rdd.RDD[U]
     */
    val rdd2 = rdd.map(f=> (f,1))
    rdd2.foreach(println)
  }
  def MapOnDF(spark:SparkSession): Unit ={

     //Syntax
    /*
    1) map[U](func : scala.Function1[T, U])(implicit evidence$6 : org.apache.spark.sql.Encoder[U])
        : org.apache.spark.sql.Dataset[U]
    2) map[U](func : org.apache.spark.api.java.function.MapFunction[T, U], encoder : org.apache.spark.sql.Encoder[U])
        : org.apache.spark.sql.Dataset[U]
     */
    val structureData =  Seq(
      Row("James","","Smith","36636","NewYork",3100),
      Row("Michael","Rose","","40288","California",4300),
      Row("Robert","","Williams","42114","Florida",1400),
      Row("Maria","Anne","Jones","39192","Florida",5500),
      Row("Jen","Mary","Brown","34561","NewYork",3000)
    )
    val structureSchema = new StructType()
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType)
      .add("id",StringType)
      .add("location",StringType)
      .add("salary",IntegerType)

    val df2 = spark.createDataFrame(
      spark.sparkContext.parallelize(structureData) , structureSchema
    )
    df2.printSchema()
    df2.show()

    import spark.implicits._

    val df3 = df2.map(
      row=>{
        val util = new Util()
        val fullname = row.getString(0) +" "+ row.getString(1) + " " + row.getString(2)
        (fullname , row.getString(3) , row.getInt(5))
      }
    )

    val df3map = df3.toDF("fullname" , "id", "salary")
    df3map.printSchema()
    df3map.show()
  }

  def flatmapOnRDD(spark:SparkSession)={

    //Syntax
    //flatMap[U](f : scala.Function1[T, scala.TraversableOnce[U]])(implicit evidence$4 : scala.reflect.ClassTag[U]) : org.apache.spark.rdd.RDD[U]

    val data = Seq("Project Gutenberg’s",
      "Alice’s Adventures in Wonderland",
      "Project Gutenberg’s",
      "Adventures in Wonderland",
      "Project Gutenberg’s")

    val rdd=spark.sparkContext.parallelize(data)
    val rdd1 = rdd.flatMap(f=>f.split(" "))
    rdd1.foreach(println)
  }

  def flatMaponDF(spark:SparkSession)={
    val arrayStructData = Seq(
        Row("James,,Smith", List("Java", "Scala", "C++"), "CA"),
        Row("Michael,Rose,", List("Spark", "Java", "C++"), "NJ"),
        Row("Robert,,Williams", List("CSharp", "VB", "R"), "NV")
    )
    val arrayStructureSchema = new StructType()
      .add("name",StringType)
      .add("languagesAtSchool", ArrayType(StringType))
      .add("currentState", StringType)

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(arrayStructData) , arrayStructureSchema
    )
    import spark.implicits._

    val df2 = df.flatMap( f=>
      f.getSeq[String](1).map(
        (f.getString(0) , _ , f.getString(2)))
    ).toDF("Name" , "language" , "state")
    df2.show()
  }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("maps").master("local").getOrCreate()
    val data = Seq("Project",
      "Gutenberg’s",
      "Alice’s",
      "Adventures",
      "in",
      "Wonderland",
      "Project",
      "Gutenberg’s",
      "Adventures",
      "in",
      "Wonderland",
      "Project",
      "Gutenberg’s")

    val rdd=spark.sparkContext.parallelize(data)
    MapOnRDD(rdd)
    MapOnDF(spark)
    flatmapOnRDD(spark)
    flatMaponDF(spark)
  }
}
