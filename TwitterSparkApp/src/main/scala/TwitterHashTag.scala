import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by hastimal on 9/21/2015.
 */

/**
 * Calculates popular hashtags (topics) over sliding 10 and 60 second windows from a Twitter
 * stream. The stream is instantiated with credentials and optionally filters supplied by the
 * command line arguments.
 *
 */
object TwitterHashTag {
  def main(args: Array[String]) {
//    if (args.length < 1) {
//      System.err.println("Usage: sbt 'run " + "[filter1] [filter2] ... [filter n]"+ "'")
//      System.exit(1)
//    }
    val filters = args

    //val (master, filters) = (args(0), args.slice(5, args.length))

    // Twitter Authentication credentials
    System.setProperty("twitter4j.oauth.consumerKey", "t0tAnvsGPStnvRJe6LPOaIjLo")
    System.setProperty("twitter4j.oauth.consumerSecret", "tSeeyiOAfBJqaR9rvAmAt8ePZA3B6YSmymXmcyqeT0FWapPAb0")
    System.setProperty("twitter4j.oauth.accessToken", "1868076104-Zftf4ts0hHGVsnTLYDXneAyTxNXlkoLf86vC3ez")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "GXL9hhOnC9oaSR6xwjEnCuIF01fOaHsYuxo6DH7WcFXty")

    val sparkConf = new SparkConf().setAppName("TwitterHashTag").setMaster("local[2]")
    //Create a Streaming COntext with 2 second window
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    //Using the streaming context, open a twitter stream (By the way you can also use filters)
    //Stream generates a series of random tweets
    val stream = TwitterUtils.createStream(ssc, None, filters)
    stream.print()



//    val ssc = new StreamingContext("local[2]", "TwitterPopularTags", Seconds(2),
//      System.getenv("SPARK_HOME"), StreamingContext.jarOfClass(this.getClass))
//    val stream = TwitterUtils.createStream(ssc, None, filters)
//    println("===Direct Stream Print===")
    // print the whole stream
    stream.foreachRDD(rdd=>rdd.foreach(println))

    // user details
    val users = stream.map(userJson => userJson.getUser())
    val usersCol = stream.map(user => user.getUser().getName()+ "@" + user.getUser().getLocation() + "@" + user.getUser().getFollowersCount() + "@" + user.getUser().getFriendsCount() + "@" + user.getUser().getFavouritesCount())
    println("===User Details===")
    usersCol.foreachRDD(rdd=>rdd.foreach(println))

    // tweet details
    val statuses = stream.map(tweet => tweet.getGeoLocation())
    println("===Tweet Details===")
    statuses.foreachRDD(rdd=>rdd.foreach(println))

    // getting the hashtags
    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))

    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))

    // Print popular hashtags
    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(5)
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(5)
      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
      SocketClient.sendCommandToRobot( "\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))

    })


    ssc.start()
    ssc.awaitTermination()
  }
}
