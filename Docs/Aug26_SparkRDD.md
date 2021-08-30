<h4><li>Spark Session</li> </h4>

Entry point to underlying Spark functionality in order
to programmatically create Spark RDD, DataFrame and DataSet.

Spark Session also includes all the APIs available in different contexts –
Spark Context,SQL Context,Streaming Context,Hive Context.

Multiple sessions -
Spark gives a straight forward API to create a new session which shares the same spark context.

<h4> <li>Spark Context</li></h4>
Since Spark 1.x, Spark SparkContext is an entry point to Spark and defined in org.apache.spark package
used to programmatically create Spark RDD, accumulators, and broadcast variables on the cluster.
You can create only one SparkContext per JVM.

SparkContext are also present in SparkSession
Spark session internally creates the Spark Context.

<h4><li> RDD basics</li></h4>
Spark RDD - Resilient Distributed Dataset
fundamental DS, immutable distributed collection of objects.
RDD is computed on several JVMs scattered across
multiple physical servers also called nodes in a cluster.

RDD Advantages -
In memory Processing ,Immutability ,Fault Tolerance
Lazy Evolution , partitioning , Parallelize

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
