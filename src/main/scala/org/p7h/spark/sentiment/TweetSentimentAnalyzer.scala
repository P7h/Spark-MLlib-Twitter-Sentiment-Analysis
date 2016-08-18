package org.p7h.spark.sentiment

import java.text.SimpleDateFormat
import java.util.Date

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkConf
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.p7h.spark.sentiment.corenlp.CoreNLPSentimentAnalyzer
import org.p7h.spark.sentiment.mllib.MLlibSentimentAnalyzer
import org.p7h.spark.sentiment.utils._
import redis.clients.jedis.Jedis
import twitter4j.Status
import twitter4j.auth.OAuthAuthorization

/**
  * Analyzes and predicts Twitter Sentiment in [near] real-time using Spark Streaming and Spark MLlib.
  * Uses the Naive Bayes Model created from the Training data and applies it to predict the sentiment of tweets
  * collected in real-time with Spark Streaming, whose batch is set to 20 seconds [configurable].
  * Raw tweets [compressed] and also the gist of predicted tweets are saved to the disk.
  * At the end of the batch, the gist of predicted tweets is published to Redis.
  * Any frontend app can subscribe to this Redis Channel for data visualization.
  */
// spark-submit --class "org.p7h.spark.sentiment.TweetSentimentAnalyzer" --master spark://spark:7077 mllib-tweet-sentiment-analysis-assembly-0.1.jar
object TweetSentimentAnalyzer {

  def main(args: Array[String]): Unit = {
    val ssc = StreamingContext.getActiveOrCreate(createSparkStreamingContext)
    val simpleDateFormat = new SimpleDateFormat("EE MMM dd HH:mm:ss ZZ yyyy")

    LogUtils.setLogLevels(ssc.sparkContext)

    // Load Naive Bayes Model from the location specified in the config file.
    val naiveBayesModel = NaiveBayesModel.load(ssc.sparkContext, PropertiesLoader.naiveBayesModelPath)
    val stopWordsList = ssc.sparkContext.broadcast(StopwordsLoader.loadStopWords(PropertiesLoader.nltkStopWords))

    /**
      * Predicts the sentiment of the tweet passed.
      * Invokes Stanford Core NLP and MLlib methods for identifying the tweet sentiment.
      *
      * @param status -- twitter4j.Status object.
      * @return tuple with Tweet ID, Tweet Text, Core NLP Polarity, MLlib Polarity, Latitude, Longitude, Profile Image URL, Tweet Date.
      */
    def predictSentiment(status: Status): (Long, String, String, Int, Int, Double, Double, String, String) = {
      val tweetText = replaceNewLines(status.getText)
      val (corenlpSentiment, mllibSentiment) = {
        // If tweet is in English, compute the sentiment by MLlib and also with Stanford CoreNLP.
        if (isTweetInEnglish(status)) {
          (CoreNLPSentimentAnalyzer.computeWeightedSentiment(tweetText),
            MLlibSentimentAnalyzer.computeSentiment(tweetText, stopWordsList, naiveBayesModel))
        } else {
          // TODO: all non-English tweets are defaulted to neutral.
          // TODO: this is a workaround :: as we cant compute the sentiment of non-English tweets with our current model.
          (0, 0)
        }
      }
      (status.getId,
        status.getUser.getScreenName,
        tweetText,
        corenlpSentiment,
        mllibSentiment,
        status.getGeoLocation.getLatitude,
        status.getGeoLocation.getLongitude,
        status.getUser.getOriginalProfileImageURL,
        simpleDateFormat.format(status.getCreatedAt))
    }

    val oAuth: Some[OAuthAuthorization] = OAuthUtils.bootstrapTwitterOAuth()
    val rawTweets = TwitterUtils.createStream(ssc, oAuth)

    // Save Raw tweets only if the flag is set to true.
    if (PropertiesLoader.saveRawTweets) {
      rawTweets.cache()

      rawTweets.foreachRDD { rdd =>
        if (rdd != null && !rdd.isEmpty() && !rdd.partitions.isEmpty) {
          saveRawTweetsInJSONFormat(rdd, PropertiesLoader.tweetsRawPath)
        }
      }
    }

    // This delimiter was chosen as the probability of this character appearing in tweets is very less.
    val DELIMITER = "¦"
    val tweetsClassifiedPath = PropertiesLoader.tweetsClassifiedPath
    val classifiedTweets = rawTweets.filter(hasGeoLocation)
      .map(predictSentiment)

    classifiedTweets.foreachRDD { rdd =>
      if (rdd != null && !rdd.isEmpty() && !rdd.partitions.isEmpty) {
        saveClassifiedTweets(rdd, tweetsClassifiedPath)

        // Now publish the data to Redis.
        rdd.foreach {
          case (id, screenName, text, sent1, sent2, lat, long, profileURL, date) => {
            val sentimentTuple = (id, screenName, text, sent1, sent2, lat, long, profileURL, date)
            // TODO -- Need to remove this and use "Spark-Redis" package for publishing to Redis.
            // TODO -- But could not figure out a way to Publish with "Spark-Redis" package though.
            // TODO -- Confirmed with the developer of "Spark-Redis" package that they have deliberately omitted the method for publishing to Redis from Spark.
            val jedis = new Jedis("localhost", 6379)
            val pipeline = jedis.pipelined
            val write = sentimentTuple.productIterator.mkString(DELIMITER)
            val p1 = pipeline.publish("TweetChannel", write)
            //println(p1.get().longValue())
            pipeline.sync()
          }
        }
      }
    }

    ssc.start()
    ssc.awaitTerminationOrTimeout(PropertiesLoader.totalRunTimeInMinutes * 60 * 1000) // auto-kill after processing rawTweets for n mins.
  }

  /**
    * Create StreamingContext.
    * Future extension: enable checkpointing to HDFS [is it really required??].
    *
    * @return StreamingContext
    */
  def createSparkStreamingContext(): StreamingContext = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      // Use KryoSerializer for serializing objects as JavaSerializer is too slow.
      .set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
      // For reconstructing the Web UI after the application has finished.
      .set("spark.eventLog.enabled", "true")
      // Reduce the RDD memory usage of Spark and improving GC behavior.
      .set("spark.streaming.unpersist", "true")

    val ssc = new StreamingContext(conf, Durations.seconds(PropertiesLoader.microBatchTimeInSeconds))
    ssc
  }

  /**
    * Saves the classified tweets to the csv file.
    * Uses DataFrames to accomplish this task.
    *
    * @param rdd                  tuple with Tweet ID, Tweet Text, Core NLP Polarity, MLlib Polarity, Latitude, Longitude, Profile Image URL, Tweet Date.
    * @param tweetsClassifiedPath Location of saving the data.
    */
  def saveClassifiedTweets(rdd: RDD[(Long, String, String, Int, Int, Double, Double, String, String)], tweetsClassifiedPath: String) = {
    val now = "%tY%<tm%<td%<tH%<tM%<tS" format new Date
    val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
    import sqlContext.implicits._
    val classifiedTweetsDF = rdd.toDF("ID", "ScreenName", "Text", "CoreNLP", "MLlib", "Latitude", "Longitude", "ProfileURL", "Date")
    classifiedTweetsDF.coalesce(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", "\t")
      // Compression codec to compress when saving to file.
      .option("codec", classOf[GzipCodec].getCanonicalName)
      // Will it be good if DF is partitioned by Sentiment value? Probably does not make sense.
      //.partitionBy("Sentiment")
      // TODO :: Bug in spark-csv package :: Append Mode does not work for CSV: https://github.com/databricks/spark-csv/issues/122
      //.mode(SaveMode.Append)
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

  /**
    * Removes all new lines from the text passed.
    *
    * @param tweetText -- Complete text of a tweet.
    * @return String without new lines.
    */
  def replaceNewLines(tweetText: String): String = {
    tweetText.replaceAll("\n", "")
  }

  /**
    * Checks if the tweet Status is in English language.
    * Actually uses profile's language as well as the Twitter ML predicted language to be sure that this tweet is
    * indeed English.
    *
    * @param status twitter4j Status object
    * @return Boolean status of tweet in English or not.
    */
  def isTweetInEnglish(status: Status): Boolean = {
    status.getLang == "en" && status.getUser.getLang == "en"
  }

  /**
    * Checks if the tweet Status has Geo-Coordinates.
    *
    * @param status twitter4j Status object
    * @return Boolean status of presence of Geolocation of the tweet.
    */
  def hasGeoLocation(status: Status): Boolean = {
    null != status.getGeoLocation
  }
}