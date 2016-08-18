package org.p7h.spark.sentiment.utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Lazily instantiated singleton instance of SQLContext.
  */
object SQLContextSingleton {

  @transient
  @volatile private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = SQLContext.getOrCreate(sparkContext)
        }
      }
    }
    instance
  }
}