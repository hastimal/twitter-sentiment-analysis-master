/**
 * Created by hastimal on 9/21/2015.
 */
//import org.apache.spark.sql.hive._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.hive._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._


object TwitterSentimentAnalysis {
  System.setProperty("hadoop.home.dir","F:\\winutils")
  Logger.getRootLogger.setLevel(Level.WARN)
  //Class that needs to be registered must be outside of the main class

  case class Person(key: Int, value: String)

  def main(args: Array[String]) {

    //val filters = args
    //val filters = Array("ps3", "ps4", "playstation", "sony", "vita", "psvita")
    val filters = Array("food", "nutrition", "diet", "healthy", "diseasefree", "physician")
    //val filers = "ThisIsSparkStreamingFilter_100K_per_Second"

    val delimeter = "|"

    System.setProperty("twitter4j.oauth.consumerKey", "t0tAnvsGPStnvRJe6LPOaIjLo")
    System.setProperty("twitter4j.oauth.consumerSecret", "tSeeyiOAfBJqaR9rvAmAt8ePZA3B6YSmymXmcyqeT0FWapPAb0")
    System.setProperty("twitter4j.oauth.accessToken", "1868076104-Zftf4ts0hHGVsnTLYDXneAyTxNXlkoLf86vC3ez")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "GXL9hhOnC9oaSR6xwjEnCuIF01fOaHsYuxo6DH7WcFXty")

    System.setProperty("twitter4j.http.useSSL", "true")

    val conf = new SparkConf().setAppName("TwitterApp").setMaster("local[4]")//.set("spark.eventLog.enabled", "true")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))

    val tweetStream = TwitterUtils.createStream(ssc, None, filters)

    val tweetRecords = tweetStream.map(status =>
    {

      def getValStr(x: Any): String = { if (x != null && !x.toString.isEmpty()) x.toString + "|" else "|" }

      /* Hive table format
       *
      create table tweeter_data
      (
      tweet_user_Id bigint
      , tweet_user_ScreenName string
      , tweet_user_FriendsCount int
      , tweet_user_FavouritesCount int
      , tweet_user_FollowersCount int
      , tweet_user_Lang string
      , tweet_user_Location string
      , tweet_user_Name string
      , tweet_Id bigint
      , tweet_CreatedAt timestamp
      , tweet_GeoLocation string
      , tweet_InReplyToUserId bigint
      , tweet_Place string
      , tweet_RetweetCount int
      , tweet_RetweetedStatus string
      , tweet_Source string
      , tweet_InReplyToScreenName string
      , tweet_Text string
      )
      ROW FORMAT DELIMITED
      FIELDS TERMINATED BY '|'
      STORED AS TEXTFILE;


      Sample data

      123456789|tweet_user_screenname|123|456|789|tweet_user_lang|tweet_user_location|tweet_user_name|34873648364|1985-09-25 17:45:30.005|tweet_geolocation|23423423|tweet_place|20|tweet_retweetedstatus|tweet_source|tweet_inreplytoscreenname|tweet_text

       *
       */
      var tweetRecord =
        getValStr(status.getUser().getId()) +
          getValStr(status.getUser().getScreenName()) +
          getValStr(status.getUser().getFriendsCount()) +
          getValStr(status.getUser().getFavouritesCount()) +
          getValStr(status.getUser().getFollowersCount()) +
          getValStr(status.getUser().getLang()) +
          getValStr(status.getUser().getLocation()) +
          getValStr(status.getUser().getName()) +
          getValStr(status.getId()) +
          getValStr(status.getCreatedAt()) +
          getValStr(status.getGeoLocation()) +
          getValStr(status.getInReplyToUserId()) +
          getValStr(status.getPlace()) +
          getValStr(status.getRetweetCount()) +
          getValStr(status.getRetweetedStatus()) +
          getValStr(status.getSource()) +
          getValStr(status.getInReplyToScreenName()) +
          getValStr(status.getText())

      tweetRecord

    })

    tweetRecords.print

    tweetRecords.filter(t => (t.length > 0)).saveAsTextFiles("src/main/resources/output/TwitterSentimentaAnalysis.txt", "data")
  //  tweetRecords.filter(t => (t.getLength() > 0)).saveAsTextFiles("/user/hive/warehouse/social.db/tweeter_data/tweets", "data")
   // tweetRecords.filter(t => (t.getLength() > 0)).saveAsTextFiles("/user/hive/warehouse/social.db/tweeter_data/tweets", "data")

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }

}