import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object ApacheSpark {
  /*
  Watching a video series to get better with apache spark in scala

  Batch Analysis:
    Data collected over a certain period of time (Historical)

  Real Time Analytics:
    Immediate data for instant results
    (Credit Cards, Stock Market Analysis, Gov)


  Spark is much faster Hadoop(Map Reduce)


   Hadoop:
   Mapper is just adding one to a specific use case (a,1)(b,1)(c,1)
   Then it sorts/shuffles the data together (kind of like a group by)
   Many I/O between the disk and memory (The main problem)

   Apache Spark:
   The entry point for spark is the spark context (SC)
   Three files split into different machines called an RDD

   RDD:
    distributed data sitting in memory
    Resilient(Real Life) distributed data
    If you lose a machine it will transfer the data to a new machine
    RDD's are immutable - once in memory you will not be able to make any changes
    Spark is faster because our data is always in memory

    Lazy Evaluation:
    The variable will only be called into memory when you call it
    It will be initially empty until something calls it in this case
    once we filter on the "df" below


   */

  def main(args: Array[String]): Unit =  {
    // Starting Spark
    val sc = new SparkContext( "local", "SparkTestInScala")
    val spark = SparkSession.builder().master("local").getOrCreate()

    import spark.implicits._

    //Reading in a text file
    val text = spark.read.format("csv").option("header", "true").load("/Users/salarchitetto/Desktop/Github/Streams/data.csv")
    val df = text.toDF()

    //Showing the structure and the dataframe
    println(df.show)
    println(df.printSchema())


    // filter1
    var filter1 = df.filter($"Overall" > 90)

    println(filter1.count())

  }
}
