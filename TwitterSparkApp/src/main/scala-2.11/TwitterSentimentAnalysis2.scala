/**
 * Created by hastimal on 9/22/2015.
 */

import com.cybozu.labs.langdetect.DetectorFactory
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.elasticsearch.spark._
import scala.util.Try

object TwitterSentimentAnalysis2 {

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","F:\\winutils")
    val filters = Array("food", "nutrition", "diet", "healthy", "diseasefree", "physician")
//    if (args.length < 4) {
//      System.err.println("Usage: TwitterSentimentAnalysis <consumer key> <consumer secret> " +
//        "<access token> <access token secret> [<filters>]")
//      System.exit(1)
//    }

    LogUtils.setStreamingLogLevels()

    DetectorFactory.loadProfile("src/main/resources/profiles")

    //val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
   // val filters = args.takeRight(args.length - 4)
    //val filters = args
    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
//    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
//    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
//    System.setProperty("twitter4j.oauth.accessToken", accessToken)
//    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
    System.setProperty("twitter4j.oauth.consumerKey", "t0tAnvsGPStnvRJe6LPOaIjLo")
    System.setProperty("twitter4j.oauth.consumerSecret", "tSeeyiOAfBJqaR9rvAmAt8ePZA3B6YSmymXmcyqeT0FWapPAb0")
    System.setProperty("twitter4j.oauth.accessToken", "1868076104-Zftf4ts0hHGVsnTLYDXneAyTxNXlkoLf86vC3ez")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "GXL9hhOnC9oaSR6xwjEnCuIF01fOaHsYuxo6DH7WcFXty")


    val sparkConf = new SparkConf().setAppName("TwitterSentimentAnalysis").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val tweets = TwitterUtils.createStream(ssc, None, filters)

    tweets.print()

    tweets.foreachRDD{(rdd, time) =>
      rdd.map(t => {
        Map(
          "user"-> t.getUser.getScreenName,
          "created_at" -> t.getCreatedAt.toInstant.toString,
          "location" -> Option(t.getGeoLocation).map(geo => { s"${geo.getLatitude},${geo.getLongitude}" }),
          "text" -> t.getText,
          "hashtags" -> t.getHashtagEntities.map(_.getText),
          "retweet" -> t.getRetweetCount,
          "language" -> detectLanguage(t.getText).toUpperCase(),
          "sentiment" -> SentimentAnalysisUtils.detectSentiment(t.getText).toString
        )
        println(("text::"+t.getText+"    "+"language::"+detectLanguage(t.getText).toUpperCase()+"    "+"sentiment::"+SentimentAnalysisUtils.detectSentiment(t.getText).toString))
        SocketClient.sendCommandToRobot(("text::"+t.getText+"    "+"language::"+detectLanguage(t.getText).toUpperCase()+"    "+"sentiment::"+SentimentAnalysisUtils.detectSentiment(t.getText).toString))
      }

      ).saveAsTextFile("src/main/resources/output2/TwitterSentimentaAnalysis.txt")
     // val topList = rdd.take(10)//10
     // SocketClient.sendCommandToRobot( rdd.count() +" is the total tweets sentimentally analyzed with Spark ")

    }
    //val topList = rdd.toString.take(1)//10
    //val topList = rdd.take(10)//10
   // SocketClient.sendCommandToRobot( rdd.count() +" is the total tweets analyzed")
    ssc.start()
    ssc.awaitTermination()

  }

  def detectLanguage(text: String) : String = {

    Try {
      val detector = DetectorFactory.create()
      detector.append(text)
      detector.detect()
    }.getOrElse("unknown")

  }
}
