package org.p7h.spark.sentiment.corenlp

import java.util.Date

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.p7h.spark.sentiment.utils._
import redis.clients.jedis.Jedis
import twitter4j.Status
import twitter4j.auth.OAuthAuthorization

// spark-submit --class "org.p7h.spark.corenlp.sentiment.SparkCoreNLPTweetSentimentAnalyzer" --master spark://localhost:7077 spark-corenlp-compute-tweet-sentiment-assembly-0.1.jar

// If running using a simple jar without assembly and with all the required jars present on the local hard-drive.
// spark-submit --jars $(echo /home/hduser/*.jar | tr ' ' ',') --class "org.p7h.spark.corenlp.sentiment.SparkCoreNLPTweetSentimentAnalyzer" --master spark://ub1404:7077 spark-corenlp-compute-tweet-sentiment_2.10-0.1.jar
object SparkCoreNLPTweetSentimentAnalyzer extends App {

  override def main(args: Array[String]): Unit = {
    val ssc = StreamingContext.getActiveOrCreate(createSparkStreamingContext)

    LogUtils.setLogLevels(ssc.sparkContext)

    def replaceNewLines(tweetText: String): String = {
      tweetText.replaceAll("\n", "")
    }

    val oAuth: Some[OAuthAuthorization] = OAuthUtils.bootstrapTwitterOAuth()
    // val filters = Array("Apache Spark", "Apache Storm", "Apache Flink")
    val rawTweets = TwitterUtils.createStream(ssc, oAuth) //, filters)
    rawTweets.cache()

    rawTweets.foreachRDD { rdd =>
      if (rdd != null && !rdd.isEmpty() && !rdd.partitions.isEmpty) {
        saveRawTweetsInJSONFormat(rdd, PropertiesLoader.tweetsRawPath)
      }
    }

    val DELIMITER = "|"
    val classifiedTweets = rawTweets.filter(status => hasGeoLocation(status) && isTweetInEnglish(status))
      .map(status => (status.getId, status.getUser.getScreenName, replaceNewLines(status.getText), CoreNLPSentimentAnalyzer.computeSentiment(status.getText), status.getGeoLocation.getLatitude, status.getGeoLocation.getLongitude))
    classifiedTweets.foreachRDD { rdd =>
      if (rdd != null && !rdd.isEmpty() && !rdd.partitions.isEmpty) {
        //val DELIMITER = "|"
        saveClassifiedTweets(PropertiesLoader.tweetsClassifiedPath, rdd)

        // Now publish the data to Redis.
        rdd.foreach {
          case (id, screenName, text, sentiment, lat, long) => {
            val tup = (id, screenName, text, sentiment, lat, long)
            // TODO -- Temporary fix. 
            // Need to remove this and use "Spark-Redis" package for publishing to Redis.
            val jedis = new Jedis("localhost", 6379)
            val pipeline = jedis.pipelined
            //val write = id + DELIMITER + screenName + DELIMITER + text + DELIMITER + sentiment + DELIMITER + lat + DELIMITER + long
            val write = tup.productIterator.mkString(DELIMITER)
            val p1 = pipeline.publish("TweetChannel", write)
            //println(p1.get().longValue())
            pipeline.sync()
          }
        }
      }
    }

    ssc.start()
    ssc.awaitTerminationOrTimeout(PropertiesLoader.totalRunTimeInMinutes * 60 * 1000) //auto-kill after processing tweets for 'n' mins.
  }

  /**
    * Create StreamingContext.
    * Future extension: enable checkpointing to HDFS [is it really reqd??].
    *
    * @return StreamingContext
    */
  def createSparkStreamingContext(): StreamingContext = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .set("spark.streaming.unpersist", "true")
      .set("spark.eventLog.enabled", "false")
      .set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
    val ssc = new StreamingContext(conf, Durations.seconds(PropertiesLoader.microBatchTimeInSeconds)) // 'n' Seconds batch interval
    ssc
  }

  def saveClassifiedTweets(tweetsClassifiedPath: String, rdd: RDD[(Long, String, String, Int, Double, Double)]): Unit = {
    /*status.getId + DELIMITER + "@" + status.getUser.getScreenName + DELIMITER + status.getText + DELIMITER +
      weightedSentiment + DELIMITER + status.getGeoLocation.getLatitude + DELIMITER +
      status.getGeoLocation.getLongitude*/

    val now = "%tY%<tm%<td%<tH%<tM%<tS" format new Date
    val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
    import sqlContext.implicits._
    val classifiedTweetsDF = rdd.toDF("ID", "ScreenName", "Text", "Sentiment", "Latitude", "Longitude")
    classifiedTweetsDF.coalesce(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", "\t")
      //.partitionBy("Sentiment") //Will it be good if DF is partitioned by Sentiment value?
      //.mode(SaveMode.Append) //Bug::Append Mode does not work for CSV: https://github.com/databricks/spark-csv/issues/122
      .save(tweetsClassifiedPath + now)
  }


  /**
    * Jackson Object Mapper for mapping twitter4j.Status object to a String for saving raw tweet.
    */
  val jacksonObjectMapper: ObjectMapper = new ObjectMapper()

  /**
    * Saves raw tweets received from Twitter Streaming API in
    *
    * @param rdd           -- RDD of Status objects to save.
    * @param tweetsRawPath -- Path of the folder where raw tweets are saved.
    */
  def saveRawTweetsInJSONFormat(rdd: RDD[Status], tweetsRawPath: String): Unit = {
    val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
    val tweet = rdd.map(status => jacksonObjectMapper.writeValueAsString(status))
    val rawTweetsDF = sqlContext.read.json(tweet)
    rawTweetsDF.coalesce(1).write
      .format("org.apache.spark.sql.json")
      // Compression codec to compress when saving to file.
      .option("codec", classOf[GzipCodec].getCanonicalName)
      .mode(SaveMode.Append)
      .save(tweetsRawPath)
  }

  def isTweetInEnglish(status: Status): Boolean = {
    status.getLang == "en" && status.getUser.getLang == "en"
  }

  def hasGeoLocation(status: Status): Boolean = {
    null != status.getGeoLocation
  }
}
