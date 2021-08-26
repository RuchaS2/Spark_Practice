import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SparkTest {
  def main(args:Array[String])={
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples")
      .getOrCreate();
    println(spark.version)
  }

}