import java.util.Properties
 
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations

import scala.collection.convert.wrapAll._
import scala.collection.mutable.ListBuffer

class SentimentAnalyzer {

    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
    val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)

    def analyzeSentence (s: String) : String = { 

        val processed = pipeline.process(s)
        val sentences = processed.get(classOf[CoreAnnotations.SentencesAnnotation])
        
        // need to check empty?
        val numberSentences = sentences.size
        println(s"numberSentences= $numberSentences")
        var sentimentScores : ListBuffer[Int] = ListBuffer()
        sentences.map(sentence => {
            var tree = sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])
            var sentiment = RNNCoreAnnotations.getPredictedClass(tree)
            println(s"sentiment score: $sentiment")

            sentimentScores.append(sentiment)
            
        })
        
        val totalSentimentScore = sentimentScores.sum
        
        val averageSentiment = totalSentimentScore / numberSentences
        
        val sentimentString = sentimentScores.mkString(",")
        
        var myJson = raw"""{"AVERAGE_SENTIMENT" : ${averageSentiment},"NUMBER_SENTENCES" : ${numberSentences}, "SENTIMENTS" : [${sentimentString}]}"""
        
        myJson
    }
}
