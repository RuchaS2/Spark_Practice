<h4><li>Spark Session</li> </h4>

Entry point to underlying Spark functionality in order
to programmatically create Spark RDD, DataFrame and DataSet.
Spark Session also includes all the APIs available in different contexts –
Spark Context,SQL Context,Streaming Context,Hive Context.

val spark = SparkSession.builder()<br>
.master("local[1]")<br>
.appName("SparkExamples")<br>
.getOrCreate();<br>

master() – If you are running it on the cluster you need to use your master name as an argument to master(). usually, it would be either yarn or mesos depends on your cluster setup.

Use local[x] when running in Standalone mode. x should be an integer value and should be greater than 0; this represents how many partitions it should create when using RDD, DataFrame, and Dataset. Ideally, x value should be the number of CPU cores you have.
appName() – Used to set your application name.

getOrCreate() – This returns a SparkSession object if already exists, creates new one if not exists.

Multiple sessions -
Spark gives a straight forward API to create a new session which shares the same spark context.

<h4> <li>Spark Context</li></h4>
Since Spark 1.x, Spark SparkContext is an entry point to Spark and defined in org.apache.spark package
used to programmatically create Spark RDD, accumulators, and broadcast variables on the cluster.
You can create only one SparkContext per JVM.

SparkContext are also present in SparkSession
Spark session internally creates the Spark Context.
first you need to create a SparkConf instance by assigning app name and setting master by using the SparkConf static methods setAppName() and setMaster() respectively and then pass SparkConf object as an argument to SparkContext constructor to create Spark Context.

getOrCreate() to create SparkContext. This function is used to get or instantiate a SparkContext and register it as a singleton object.
<h4><li> RDD basics</li></h4>
Spark RDD - Resilient Distributed Dataset
fundamental DS, immutable distributed collection of objects.
RDD is computed on several JVMs scattered across
multiple physical servers also called nodes in a cluster.

RDD Advantages -
In memory Processing ,Immutability ,Fault Tolerance
Lazy Evolution , partitioning , Parallelize

 - Ways to create an RDD in Apache Spark:

Spark create RDD from Seq or List  (using Parallelize)<br>
Creating an RDD from a text file<br>
Creating from another RDD<br>
Creating from existing DataFrames and DataSet<br>

<h4><li> Dataset and Dataframes</li></h4>
DataFrames -
Spark Dataframes are the distributed collection of the data points,
but here, the data is organized into the named columns. 
They allow developers to debug the code during the runtime 
which was not allowed with the RDDs.
It uses a catalyst optimizer for optimization purposes
<br>

Dataset -
Spark Datasets is an extension of Dataframes API 
with the benefits of both RDDs and the Datasets. 
Type safety means that the compiler will validate the data types 
of all the columns in the dataset while compilation 
only and will throw an error if there 
is any mismatch in the data types.

<h4><li>iCode Session - Strati Lab</li></h4>
