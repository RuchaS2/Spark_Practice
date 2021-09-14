package SparkRDD

import org.apache.spark.sql.SparkSession
/*
  Cache , persist -
  optimization mechanism to store the intermediate computation of an RDD, DataFrame, and Dataset so they can be reused in subsequent actions
  RDD cache() method default saves it to memory (MEMORY_ONLY)
  persist() method is used to store it to user-defined storage level.
  When you persist a dataset, each node stores it’s partitioned data in memory and reuses them in other actions on that dataset.
  Spark’s persisted data on nodes are fault-tolerant
  Advantages - cost, time efficient, less execution time
  Caching of Spark DataFrame or Dataset is a lazy operation

  Unpersist - remove from the memory or storage.
  Storage Level    Space used  CPU time  In memory  On-disk  Serialized   Recompute some partitions
  ----------------------------------------------------------------------------------------------------
  MEMORY_ONLY          High        Low       Y          N        N         Y
  MEMORY_ONLY_SER      Low         High      Y          N        Y         Y
  MEMORY_AND_DISK      High        Medium    Some       Some     Some      N
  MEMORY_AND_DISK_SER  Low         High      Some       Some     Y         N
  DISK_ONLY            Low         High      N          Y        Y         N

 Other Persistence Storage Levels
 Memory only and Replicate , Serialized in Memory and Replicate , Memory, Disk and Replicate,
 Serialize in Memory, Disk and Replicate ,
 */

object Cache_Persist {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("cache").master("local[1]").getOrCreate()

    import spark.implicits._
    val cols = Seq("No","Quote")
    val data =  Seq(("1", "Be the change that you wish to see in the world"),
      ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
      ("3", "The purpose of our lives is to be happy."))

    val df = data.toDF(cols:_*)
    val dfcache = df.cache()
    dfcache.show(false)

    val dfPersist = df.persist()
    dfPersist.show(false)

    val dfunPersist = dfPersist.unpersist()
    dfunPersist.show(false)
  }
}
