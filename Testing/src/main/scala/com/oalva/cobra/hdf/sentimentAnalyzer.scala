package com.oalva.cobra.hdf

import java.util.Properties
import scala.collection.JavaConversions._
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.sentiment._
import org.apache.spark.sql.functions._

object sentimentAnalyzer {
  
  
    val coreNLP = new StanfordCoreNLP(initialize)
  
    val predictSentiment = (text :String) =>{
    val sentiments = Array("strongly negative","negative", "neutral", "positive", "strongly positive")
    //val coreNLP = new StanfordCoreNLP(initialize)
    val annotations = coreNLP.process(text)
    val sentences = annotations.get(classOf[CoreAnnotations.SentencesAnnotation])
    var sentiment = -1
    for (sentence <- sentences) {
      val tree = sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])
      sentiment =RNNCoreAnnotations.getPredictedClass(tree)
      //println(sentence+"__"+sentiment)
    }
    sentiments(sentiment)
  }
    
    def sentimentScore(score : List[Int]) : Int ={
    var scoreSum =0
    score.foreach(x => scoreSum = scoreSum + x )
    val meanScore = (scoreSum.toDouble / score.length.toDouble).round.toInt
    meanScore
  }
    
    def initialize : Properties ={
    val props = new Properties()
    props.put("annotators", "tokenize,ssplit,pos,lemma,parse, sentiment")
    props.setProperty("ssplit.eolonly", "true")
    props.setProperty("tokenize.whitespace", "true")
    props
  }
    
    val predictSentimentUDF = udf(predictSentiment)
  

  
}