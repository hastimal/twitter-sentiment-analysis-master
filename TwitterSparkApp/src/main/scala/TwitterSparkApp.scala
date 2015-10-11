import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.twitter.TwitterUtils

/**
 * Created by hastimal on 9/21/2015.
 */
object TwitterSparkApp {
     def main(args: Array[String]) {

      val filters = args

      // Set the system properties so that Twitter4j library used by twitter stream
      // can use them to generate OAuth credentials

      System.setProperty("twitter4j.oauth.consumerKey", "t0tAnvsGPStnvRJe6LPOaIjLo")
      System.setProperty("twitter4j.oauth.consumerSecret", "tSeeyiOAfBJqaR9rvAmAt8ePZA3B6YSmymXmcyqeT0FWapPAb0")
      System.setProperty("twitter4j.oauth.accessToken", "1868076104-Zftf4ts0hHGVsnTLYDXneAyTxNXlkoLf86vC3ez")
      System.setProperty("twitter4j.oauth.accessTokenSecret", "GXL9hhOnC9oaSR6xwjEnCuIF01fOaHsYuxo6DH7WcFXty")

      //Create a spark configuration with a custom name and master
      // For more master configuration see  https://spark.apache.org/docs/1.2.0/submitting-applications.html#master-urls
      val sparkConf = new SparkConf().setAppName("TwitterStreamingRealTime").setMaster("local[*]")
      //Create a Streaming COntext with 2 second window
      val sc = new StreamingContext(sparkConf, Seconds(2))
      //Using the streaming context, open a twitter stream (By the way you can also use filters)
      //Stream generates a series of random tweets
      val stream = TwitterUtils.createStream(sc, None, filters)
      stream.print()
      //Map : Retrieving Hash Tags
      val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

      //Finding the top hash Tags on 30 second window
      val topCounts30 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(30))
        .map{case (topic, count) => (count, topic)}
        .transform(_.sortByKey(false))
      //Finding the top hash Tgas on 10 second window
      val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
        .map{case (topic, count) => (count, topic)}
        .transform(_.sortByKey(false))

      // Print popular hashtags
      topCounts30.foreachRDD(rdd => {
        val topList = rdd.take(10)
        println("\nPopular topics in last 30 seconds (%s total):".format(rdd.count()))
        topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
        //topCounts30.saveAsTextFiles("src/main/resources/output/PopularTopics30.txt")
      })

      topCounts10.foreachRDD(rdd => {
        val topList = rdd.take(10)
        println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
        topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
        //topCounts10.saveAsTextFiles("src/main/resources/output/PopularTopics10.txt")
      })
      sc.start()

      sc.awaitTermination()
    }
  }
