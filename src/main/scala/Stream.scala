import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import java.io.{File, PrintWriter}

object Stream {

  def main(args: Array[String]) {
      //Logger
      val writer = new PrintWriter(new File("output.log"))

      val Spark = SparkSession
        .builder()
        .appName("Twitter Stream Data")
        .config("spark.master", "local")
        .getOrCreate()

      import Spark.implicits._
      val cb = new ConfigurationBuilder
      cb.setDebugEnabled(true).setOAuthConsumerKey("8ZIH6tvAcamgasHU7SEBUPZ90")
        .setOAuthConsumerSecret("l1HJQP89adiqAy9AFeWKwtZ0aqCzlt7xSvo7F1b2nRQPQsFgjs")
        .setOAuthAccessToken("971925425848029184-rxMxZuX6v34eAFnSQ8YwBlQ9iN1qWKv")
        .setOAuthAccessTokenSecret("CqBDgMHhWMAZTsIW9YDJ0GK7fwDD8RiUtViPkBm1Ehxtn")

      val appName = "Data"
      val conf = new SparkConf()
      conf.set("spark.driver.allowMultipleContexts", "true")
      conf.setAppName(appName).setMaster("local[4]")
      val ssc = new StreamingContext(conf, Minutes(1))

      val auth = new OAuthAuthorization(cb.build())
      val tweets = TwitterUtils.createStream(ssc, Some(auth))

      val words = tweets.flatMap(status => status.getText.split(" "))
      val juve = words.filter(word=>word.contains("Juventus"))
      val manUnited = words.filter(word => word.contains("Manchester"))
      val hashTags = tweets.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

      val topWords = words.map((_ , 1)).reduceByKeyAndWindow(_ + _ , Seconds(60))
        .map {case (topic, count) => (count, topic)}
        .transform(_.sortByKey(false))

      val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
        .map { case (topic, count) => (count, topic) }
        .transform(_.sortByKey(false))

      val juveCounts = juve.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
          .map {case (topic, count) => (count, topic) }
          .transform(_.sortByKey(false))

      val unitedCounts = manUnited.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
        .map {case (topic, count) => (count, topic) }
        .transform(_.sortByKey(false))


      topCounts60.foreachRDD(rdd => {
          val topList = rdd.take(10)
          writer.write("Popular topics in last 60 seconds (%s total):".format(rdd.count()))
          topList.foreach { case (count, tag) => writer.write("%s (%s tweets)".format(tag, count))}
          writer.write(s"${"*" * 50}")
      })


      juveCounts.foreachRDD(rdd => {
          val topJuve = rdd.take(10)
          writer.write(s"Juve counts in the last 60 seconds : ${rdd.count()}")
          topJuve.foreach {case (count, tag) => writer.write("%s (%s tweets)".format(tag, count))}

      })

      unitedCounts.foreachRDD(rdd => {
          val topUnited = rdd.take(10)
          writer.write(s"Man Untited counts in the last 60 seconds : ${rdd.count()}")
          topUnited.foreach {case (count, tag) => writer.write("%s (%s tweets)".format(tag, count))}
      })

      topWords.foreachRDD(rdd => {
          val topUnited = rdd.take(30)
          writer.write(s"Word counts in the last 60 seconds : ${rdd.count()}")
          topUnited.foreach {case (count, word) => writer.write("%s (%s tweets)".format(word, count))}
      })

      // Need to find out how the hell to get this into a dataframe or at least a database in Scala
      //What a pain in the ass this is

      case class top60(hashTag: String,
                       numOfTweets: String)

      case class top10(hashTag: String,
                       numOfTweets: String)

      ssc.start()
      ssc.awaitTermination()
      writer.close()

    }

}
