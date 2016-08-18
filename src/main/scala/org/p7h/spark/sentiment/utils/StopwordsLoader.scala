package org.p7h.spark.sentiment.utils

import scala.io.Source

/**
  * Helper class for loading stop words from a file ["NLTK_English_Stopwords_Corpus.txt"] from the classpath.
  */
object StopwordsLoader {

  def loadStopWords(stopWordsFileName: String): List[String] = {
    Source.fromInputStream(getClass.getResourceAsStream("/" + stopWordsFileName)).getLines().toList
  }
}