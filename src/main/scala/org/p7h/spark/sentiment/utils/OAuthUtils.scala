package org.p7h.spark.sentiment.utils

import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

/**
  * Helper class for loading Twitter App OAuth Credentials into the VM.
  * The required values for various keys are picked from a properties file in the classpath.
  */
object OAuthUtils {
  def bootstrapTwitterOAuth(): Some[OAuthAuthorization] = {
    System.setProperty("twitter4j.oauth.consumerKey", PropertiesLoader.consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", PropertiesLoader.consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", PropertiesLoader.accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", PropertiesLoader.accessTokenSecret)

    val configurationBuilder = new ConfigurationBuilder()
    val oAuth = Some(new OAuthAuthorization(configurationBuilder.build()))

    oAuth
  }
}