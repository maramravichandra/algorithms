package com.oalva.cobra.hdf

import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType, BooleanType }
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.feature.StopWordsRemover
import edu.stanford.nlp.process.Morphology
import scala.util.hashing.Hashing

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.mllib.clustering.KMeans
//import org.apache.spark.sql.DataFrame
import org.apache.log4j.LogManager
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.hive.HiveContext

object D1TweetProcessor {

  val LOG = LogManager.getRootLogger

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("d1TweetProcessor")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    // Initialize property file parameters
    val d1HDFSpath = "/ravi/data/data/";//sc.getConf.get("spark.cobra.d1HDFSpath");
    LOG.info(s"""spark.cobra.d1HDFSpath = $d1HDFSpath """);
    val d1Delimiter = "|";//sc.getConf.get("spark.cobra.d1Delimiter");
    LOG.info(s"""spark.cobra.d1Delimiter = $d1Delimiter""");
    val removeKeywordsHDFSpath = "/ravi/config/config/removeKeywords.csv";//sc.getConf.get("spark.cobra.removeKeywordsHDFSpath");
    LOG.info(s"""spark.cobra.removeKeywordsHDFSpath=$removeKeywordsHDFSpath""");
    val removeKeywordsDelimiter = "|";//sc.getConf.get("spark.cobra.removeKeywordsDelimiter");
    LOG.info(s"""spark.cobra.removeKeywordsDelimiter=$removeKeywordsDelimiter""");

    /*    val flagOneHDFSpath = sc.getConf.get("spark.cobra.flagOneHDFSpath")
    val flagOneDelimiter = sc.getConf.get("spark.cobra.flagOneDelimiter")
    val flagTwoHDFSpath = sc.getConf.get("spark.cobra.flagTwoHDFSpath")
    val flagTwoDelimiter = sc.getConf.get("spark.cobra.flagTwoDelimiter")*/

    val d1TorontoHandles = "/ravi/config/config/torontoOfficialHandlesID.csv";//sc.getConf.get("spark.cobra.d1TorontoHandles");
    LOG.info(s"""spark.cobra.d1TorontoHandles=$d1TorontoHandles""");
    val source = "twitter";//sc.getConf.get("spark.cobra.source");
    LOG.info(s"""spark.cobra.source=$source""");
    val unixTimeStampFormat = "E MMM d HH:mm:ss Z yyyy";//sc.getConf.get("spark.cobra.unixTimeStampFormat");
    LOG.info(s"""spark.cobra.unixTimeStampFormat=$unixTimeStampFormat""");
    val punctuationRegex = "[\\p{Punct}]";//sc.getConf.get("spark.cobra.punctuationRegex");
    LOG.info(s"""spark.cobra.punctuationRegex=$punctuationRegex""");
    val numbersRegex = "([0-9]+)";//sc.getConf.get("spark.cobra.numbersRegex");
    LOG.info(s"""spark.cobra.numbersRegex=$numbersRegex""");
    val urlRegex = "(http|ftp|https)://([\\w_-]+(?:(?:\\.[\\w_-]+)+))([\\w.,@?^=%&:/~+#-]*[\\w@?^=%&/~+#-])?"; //sc.getConf.get("spark.cobra.urlRegex");
    LOG.info(s"""spark.cobra.urlRegex=$urlRegex""");
    val noWhiteSpaceRegex = "^ +| +$| (?= )";//sc.getConf.get("spark.cobra.noWhiteSpaceRegex");
    LOG.info(s"""spark.cobra.noWhiteSpaceRegex=$noWhiteSpaceRegex""");
    val specialCharsRegex = "[^\\x00-\\x7F]";//sc.getConf.get("spark.cobra.specialCharsRegex");
    LOG.info(s"""spark.cobra.specialCharsRegex=$specialCharsRegex""");
    val cobraMasterTable = "ravi.ca_master";//sc.getConf.get("spark.cobra.cobraMasterTable");
    LOG.info(s"""spark.cobra.cobraMasterTable=$cobraMasterTable""");
    val cobraAspectTable = "ravi.ca_aspect";//sc.getConf.get("spark.cobra.cobraAspectTable");
    LOG.info(s"""spark.cobra.cobraAspectTable=$cobraAspectTable""");
    val cobraClusterTable = "ravi.ca_cluster";//sc.getConf.get("spark.cobra.cobraClusterTable");
    LOG.info(s"""spark.cobra.cobraClusterTable=$cobraClusterTable""");
    val cobraWordCountTable = "ravi.ca_wordcount";//sc.getConf.get("spark.cobra.cobraWordCountTable");
    LOG.info(s"""spark.cobra.cobraWordCountTable=$cobraWordCountTable""");

    val customTwitterSchema = StructType(Array(StructField("UserHandle", StringType, true),
      StructField("UserID", StringType, true),
      StructField("UserIDCreationTime", StringType, true),
      StructField("UserVerified", StringType, true),
      StructField("UserFriendsCount", StringType, true),
      StructField("UserStatusesCount", StringType, true),
      StructField("UserGeoEnabled", StringType, true),
      StructField("TweetTime", StringType, true),
      StructField("TweetLangugage", StringType, true),
      StructField("TweetRawMessage", StringType, true),
      StructField("TweetHashTags", StringType, true),
      StructField("TweetUserMentions", StringType, true),
      StructField("TweetURLs", StringType, true),
      StructField("TweetCoordinates", StringType, true),
      StructField("TweetPlaceType", StringType, true),
      StructField("TweetLocation", StringType, true),
      StructField("TweetCountry", StringType, true),
      StructField("TweetFavourited", StringType, true),
      StructField("TweetFavouriteCount", StringType, true),
      StructField("TweetRetweeted", StringType, true),
      StructField("TweetRetweetedCount", StringType, true)))

    val removeKeywordsSchema = StructType(Array(StructField("Keyword", StringType, true)))

    val twitterInitFeedDF = hiveContext.read.format("com.databricks.spark.csv")
      .options(Map("header" -> "false"))
      .schema(customTwitterSchema)
      .option("delimiter", d1Delimiter)
      .option("mode", "DROPMALFORMED").load(d1HDFSpath);
    
    LOG.info(s"""twitterInitFeedDF is loaded""");

    val removeKeywordsDF = hiveContext.read.format("com.databricks.spark.csv")
      .options(Map("header" -> "false"))
      .schema(removeKeywordsSchema)
      .option("delimiter", removeKeywordsDelimiter)
      .load(removeKeywordsHDFSpath);

    LOG.info(s"""removeKeywordsDF is loaded""");
    
    val torontoHandlesDF = hiveContext.read.format("com.databricks.spark.csv")
      .options(Map("header" -> "false"))
      .load(d1TorontoHandles);

    LOG.info(s"""torontoHandlesDF is loaded""");
    
    val torontoHandlesIDs = torontoHandlesDF.collect.map { case Row(a: String) => a.map(x => x) }
    val torontoHandlesIDsB = sc.broadcast(torontoHandlesIDs)
    val userIDExists = (userID: String) => torontoHandlesIDsB.value.exists(userID.toLowerCase().equals)
    val userIDExistsUDF = udf(userIDExists)

    val computeHashString = (text: String) => {
      java.security.MessageDigest.getInstance("MD5")
        .digest(text.getBytes())
        .map(0xFF & _)
        .map { "%02x".format(_) }
        .foldLeft("") { _ + _ }
    }
    val computeHashStringUDF = udf(computeHashString)

    val returnMasterFlag = (flagOne: String, flagTwo: String) => {
      var word = "others"
      if (flagOne != null && flagOne != "") {
        word = flagOne
      } else if (flagTwo != null && flagTwo != "") {
        word = flagTwo
      }
      word
    }

    val determineSentimentScoreUDF = udf((sentiment: String) => sentiment match {
      case "strongly negative" => -2
      case "negative"          => -1
      case "neutral"           => 0
      case "positive"          => -1
      case "strongly positive" => 2
    })

    val returnMasterFlagUDF = udf(returnMasterFlag)

    val twitterFeedDF = twitterInitFeedDF.withColumn("source", lit(source))
      .withColumn("source_flag", when(userIDExistsUDF(col("UserID")), 1).otherwise(0))
      //.withColumn("source_recordID", monotonicallyIncreasingId)
      .withColumn("source_recordID", computeHashStringUDF(concat(col("UserID"), col("tweettime"), col("TweetRawMessage"))))

    val removeKeywords = removeKeywordsDF.select("Keyword").rdd.collect().map { case Row(a: String) => a.map(x => x) }

    val twitterFeedTypeCastDF = twitterFeedDF.withColumn("TweetTime", unix_timestamp(col("TweetTime"), unixTimeStampFormat).cast("timestamp"))
      .withColumn("UserIDCreationTime", unix_timestamp(col("UserIDCreationTime"), "E MMM d HH:mm:ss Z yyyy").cast("timestamp"))
      .withColumn("UserID", 'UserID.cast("BigInt"))
      .withColumn("UserFriendsCount", 'UserFriendsCount.cast("Int"))
      .withColumn("UserStatusesCount", 'UserStatusesCount.cast("Int"))
      .withColumn("TweetFavouriteCount", 'TweetFavouriteCount.cast("Int"))
      .withColumn("TweetRetweetedCount", 'TweetRetweetedCount.cast("Int"))
      .withColumn("UserVerified", 'UserVerified.cast("Boolean"))
      .withColumn("UserGeoEnabled", 'UserGeoEnabled.cast("Boolean"))
      .withColumn("TweetFavourited", 'TweetFavourited.cast("Boolean"))
      .withColumn("TweetRetweeted", 'TweetRetweeted.cast("Boolean"))

    val noPunctuationDF = twitterFeedTypeCastDF.withColumn("TweetCleanedMessage", regexp_replace(twitterFeedTypeCastDF("TweetRawMessage"), punctuationRegex, " "))
    val noNumbersDF = noPunctuationDF.withColumn("TweetCleanedMessage", regexp_replace(noPunctuationDF("TweetCleanedMessage"), numbersRegex, ""))
    val noURLDF = noNumbersDF.withColumn("TweetCleanedMessage", regexp_replace(noNumbersDF("TweetCleanedMessage"), urlRegex, ""))
    val noWhiteSpace = noURLDF.withColumn("TweetCleanedMessage", regexp_replace(noURLDF("TweetCleanedMessage"), noWhiteSpaceRegex, ""))
    val cleanedTweetsDF = noWhiteSpace.withColumn("TweetCleanedMessage", regexp_replace(noWhiteSpace("TweetCleanedMessage"), specialCharsRegex, ""))
    val trimTweetsDF = cleanedTweetsDF.withColumn("TweetCleanedMessage", trim(cleanedTweetsDF("TweetCleanedMessage")));
    

    val wordTokenizer = new Tokenizer().setInputCol("TweetCleanedMessage").setOutputCol("TweetCleanedwords");
    val tweetTokenWordsDF = wordTokenizer transform trimTweetsDF drop "TweetCleanedMessage"

    val swRemover = new StopWordsRemover().setInputCol("TweetCleanedwords").setOutputCol("TweetCleanedwords_no_sw")
    val tweetNoSWDF = swRemover transform tweetTokenWordsDF drop "TweetCleanedwords"

    val keywordRemover = new StopWordsRemover().setInputCol("TweetCleanedwords_no_sw").setOutputCol("TweetCleanedwords_no_kw")
    keywordRemover.setStopWords(removeKeywords)
    val tweetNoKWDF = keywordRemover transform tweetNoSWDF drop "TweetCleanedwords_no_sw"

    val stemWords = udf { terms: Seq[String] => terms.map { word => (new Morphology().stem(word)) } }
    val stemmedTweetDF = tweetNoKWDF.withColumn("TweetStemmedwords", stemWords(col("TweetCleanedwords_no_kw")))

    val preprocessedTweetDF = stemmedTweetDF.withColumn("TweetCleanedMessage", concat_ws(" ", col("TweetStemmedwords")))
    val cleanedMessageDF = preprocessedTweetDF.drop(preprocessedTweetDF.col("TweetCleanedwords_no_kw")).drop(preprocessedTweetDF.col("TweetStemmedwords"))

    flagProcessor.intiliazeFlagProperty(sc)

    val flagProcessedDF = cleanedMessageDF.withColumn("basewordWithFlags", flagProcessor.getMatchingWordUDF(lower(col("TweetRawMessage"))))

    val flagDF = flagProcessedDF.withColumn("_tmp", split($"basewordWithFlags", "#"))
      .withColumn("_coordinates", split(regexp_replace(regexp_replace($"TweetCoordinates", "\\[", ""), "\\]", ""), ","))
      .select($"UserHandle", $"UserID", $"UserIDCreationTime", $"UserVerified", $"UserFriendsCount", $"UserStatusesCount",
        $"UserGeoEnabled", $"TweetTime", $"TweetLangugage", $"TweetRawMessage", $"TweetCleanedMessage", $"TweetHashTags",
        $"TweetUserMentions", $"TweetURLs", $"TweetCoordinates", $"TweetPlaceType", $"TweetLocation", $"TweetCountry", $"TweetFavourited",
        $"TweetFavouriteCount", $"TweetRetweeted", $"TweetRetweetedCount", $"source", $"source_flag", $"source_recordID",
        $"_tmp".getItem(0).as("base_word"),
        $"_tmp".getItem(1).as("combination_word"),
        $"_tmp".getItem(2).cast("Int").as("bell_flag_id"),
        $"_tmp".getItem(3).cast("Int").as("rogers_flag_id"),
        $"_tmp".getItem(4).cast("Int").as("freedom_flag_id"),
        $"_tmp".getItem(5).cast("Int").as("telus_flag_id"),
        $"_tmp".getItem(6).cast("Int").as("chatr_flag_id"),
        $"_tmp".getItem(7).cast("Int").as("fido_flag_id"),
        $"_tmp".getItem(8).cast("Int").as("kodoo_flag_id"),
        $"_tmp".getItem(9).cast("Int").as("public_flag_id"),
        $"_tmp".getItem(10).cast("Int").as("shaw_flag_id"),
        $"_tmp".getItem(11).cast("Int").as("virgin_flag_id"),
        $"_tmp".getItem(12).cast("Int").as("bbw_flag_id"),
        $"_coordinates".getItem(0).as("longitude"),
        $"_coordinates".getItem(1).as("latitude"))
      .drop("_tmp").drop("basewordWithFlags")
      .withColumn("master_word", returnMasterFlagUDF(col("base_word"), col("combination_word")))
      //.withColumn("sentiment", lit("")).withColumn("score", lit("")).withColumn("clusterid", lit(""))
      .withColumn("sentiment", sentimentAnalyzer.predictSentimentUDF(col("TweetCleanedMessage")))
      .withColumn("score", determineSentimentScoreUDF(col("sentiment")))
      .withColumn("clusterid", lit(""))
      //flagOnePath
    val flagReorderedDF = flagDF.select($"UserHandle", $"UserID", $"UserIDCreationTime", $"UserVerified", $"UserFriendsCount", $"UserStatusesCount", $"UserGeoEnabled", $"TweetTime",
      $"TweetLangugage", $"TweetRawMessage", $"TweetHashTags", $"TweetUserMentions", $"TweetURLs", $"TweetCoordinates", $"longitude", $"latitude", $"TweetPlaceType",
      $"TweetLocation", $"TweetCountry", $"TweetFavourited", $"TweetFavouriteCount", $"TweetRetweeted", $"TweetRetweetedCount", $"source", $"source_flag", $"source_recordID",
      $"TweetCleanedMessage", $"base_word", $"combination_word", $"master_word", $"bell_flag_id", $"rogers_flag_id", $"freedom_flag_id", $"telus_flag_id", $"chatr_flag_id",
      $"fido_flag_id", $"kodoo_flag_id", $"public_flag_id", $"shaw_flag_id", $"virgin_flag_id", $"bbw_flag_id", $"sentiment", $"score", $"clusterid")

    val masterDF01 =  flagReorderedDF.withColumn("general_1", lit("")).withColumn("general_2", lit("")).withColumn("general_3", lit("")).withColumn("general_4", lit("")).withColumn("general_5", lit(""))
      .withColumn("general_6", lit("")).withColumn("general_7", lit("")).withColumn("general_8", lit("")).withColumn("general_9", lit("")).withColumn("general_10", lit(""))
      .withColumn("general_11", lit("")).withColumn("general_12", lit("")).withColumn("general_13", lit("")).withColumn("general_14", lit("")).withColumn("general_15", lit(""))
      .withColumn("general_16", lit("")).withColumn("general_17", lit("")).withColumn("general_18", lit("")).withColumn("general_19", lit("")).withColumn("general_20", lit(""))
      .withColumn("general_21", lit("")).withColumn("general_22", lit("")).withColumn("general_23", lit("")).withColumn("general_24", lit("")).withColumn("general_25", lit(""))
      .withColumn("general_26", lit("")).withColumn("general_27", lit("")).withColumn("general_28", lit("")).withColumn("general_29", lit("")).withColumn("general_30", lit(""))
      .withColumn("general_31", lit("")).withColumn("general_32", lit("")).withColumn("general_33", lit("")).withColumn("general_34", lit("")).withColumn("general_35", lit(""))
      .withColumn("general_36", lit("")).withColumn("general_37", lit("")).withColumn("general_38", lit("")).withColumn("general_39", lit("")).withColumn("general_40", lit(""))
      .withColumn("general_41", lit("")).withColumn("general_42", lit("")).withColumn("general_43", lit("")).withColumn("general_44", lit("")).withColumn("general_45", lit(""))
      .withColumn("general_46", lit("")).withColumn("general_47", lit("")).withColumn("general_48", lit("")).withColumn("general_49", lit("")).withColumn("general_50", lit(""))

    val masterDF02 = masterDF01.withColumn("general_51", lit("")).withColumn("general_52", lit("")).withColumn("general_53", lit("")).withColumn("general_54", lit("")).withColumn("general_55", lit(""))
      .withColumn("general_56", lit("")).withColumn("general_57", lit("")).withColumn("general_58", lit("")).withColumn("general_59", lit("")).withColumn("general_60", lit(""))
      .withColumn("general_61", lit("")).withColumn("general_62", lit("")).withColumn("general_63", lit("")).withColumn("general_64", lit("")).withColumn("general_65", lit(""))
      .withColumn("general_66", lit("")).withColumn("general_67", lit("")).withColumn("general_68", lit("")).withColumn("general_69", lit("")).withColumn("general_70", lit(""))
      .withColumn("general_71", lit("")).withColumn("general_72", lit("")).withColumn("general_73", lit("")).withColumn("general_74", lit("")).withColumn("general_75", lit(""))
      .withColumn("general_76", lit("")).withColumn("general_77", lit("")).withColumn("general_78", lit("")).withColumn("general_79", lit("")).withColumn("general_80", lit(""))
      .withColumn("general_81", lit("")).withColumn("general_82", lit("")).withColumn("general_83", lit("")).withColumn("general_84", lit("")).withColumn("general_85", lit(""))
      .withColumn("general_86", lit("")).withColumn("general_87", lit("")).withColumn("general_88", lit("")).withColumn("general_89", lit("")).withColumn("general_90", lit(""))
      .withColumn("general_91", lit("")).withColumn("general_92", lit("")).withColumn("general_93", lit("")).withColumn("general_94", lit("")).withColumn("general_95", lit(""))
      .withColumn("general_96", lit("")).withColumn("general_97", lit("")).withColumn("general_98", lit("")).withColumn("general_99", lit("")).withColumn("general_100", lit(""))

    val masterDF03 = masterDF02.withColumn("general_101", lit("")).withColumn("general_102", lit("")).withColumn("general_103", lit("")).withColumn("general_104", lit(""))
      .withColumn("general_105", lit("")).withColumn("general_106", lit("")).withColumn("general_107", lit("")).withColumn("general_108", lit(""))
      .withColumn("general_109", lit("")).withColumn("general_110", lit("")).withColumn("general_111", lit("")).withColumn("general_112", lit(""))
      .withColumn("general_113", lit("")).withColumn("general_114", lit("")).withColumn("general_115", lit("")).withColumn("general_116", lit(""))
      .withColumn("general_117", lit("")).withColumn("general_118", lit("")).withColumn("general_119", lit("")).withColumn("general_120", lit(""))
      .withColumn("general_121", lit("")).withColumn("general_122", lit("")).withColumn("general_123", lit("")).withColumn("general_124", lit(""))
      .withColumn("general_125", lit("")).withColumn("general_126", lit("")).withColumn("general_127", lit("")).withColumn("general_128", lit(""))
      .withColumn("general_129", lit("")).withColumn("general_130", lit("")).withColumn("general_131", lit("")).withColumn("general_132", lit(""))
      .withColumn("general_133", lit("")).withColumn("general_134", lit("")).withColumn("general_135", lit("")).withColumn("general_136", lit(""))
      .withColumn("general_137", lit("")).withColumn("general_138", lit("")).withColumn("general_139", lit("")).withColumn("general_140", lit(""))
      .withColumn("general_141", lit("")).withColumn("general_142", lit("")).withColumn("general_143", lit("")).withColumn("general_144", lit(""))
      .withColumn("general_145", lit("")).withColumn("general_146", lit("")).withColumn("general_147", lit("")).withColumn("general_148", lit(""))
      .withColumn("general_149", lit("")).withColumn("general_150", lit(""))

    //y.insertInto(hiveD2TableName) addAdditionalColumns( 1,150,flagReorderedDF,hiveContext);
    val masterDF = masterDF03.dropDuplicates(Array("UserID", "TweetTime", "TweetRawMessage"))
    //val masterDF = addAdditionalColumns( 1,150,flagReorderedDF,hiveContext).dropDuplicates(Array("UserID", "TweetTime", "TweetRawMessage"))
    masterDF.write.mode("append").saveAsTable(cobraMasterTable)

    aspectScorer.intiliazeAskProperty(sc);
    LOG.info(s"""Ask propeperty file has been read""");

    val aspectBaseDF = masterDF.select("tweetRawMessage", "source_recordID")
    //val aspectBaseDF = hiveContext.sql("select tweetRawMessage,source_recordID from adv_analytics.CA_Master")

    val aspectDF = aspectBaseDF.withColumn("aspectValueString", aspectScorer.getAspectValueUDF(lower(col("TweetRawMessage"))))
      .withColumn("_tmp", split($"aspectValueString", "#"))
      .select($"source_recordID",
        $"aspectValueString", $"_tmp".getItem(0).as("ce"),
        $"_tmp".getItem(1).as("commercial"),
        $"_tmp".getItem(2).as("company general"),
        $"_tmp".getItem(3).as("competition"),
        $"_tmp".getItem(4).as("events"),
        $"_tmp".getItem(5).as("handset"),
        $"_tmp".getItem(6).as("network"),
        $"_tmp".getItem(7).as("product/services"),
        $"_tmp".getItem(8).as("aspectMax"))
      .drop("_tmp")
      .drop("aspectValueString")

    //aspectDF.insertInto(cobraAspectTable)
    aspectDF.write.mode("append").saveAsTable(cobraAspectTable)
//================================================
    val clusterDF = masterDF.select("tweetCleanedMessage", "source_recordID")
    //val d3DF = hiveContext.sql("select tweetCleanedMessage,source_recordID from adv_analytics.CA_Master")
    

    val mlTokenizer = new Tokenizer().setInputCol("tweetCleanedMessage").setOutputCol("tweetWords")
    val mlTokenDF = mlTokenizer transform clusterDF

    val word2Vec = new Word2Vec().setInputCol("tweetWords").setOutputCol("VectorResult").setVectorSize(3).setMinCount(0)
    val model = word2Vec.fit(mlTokenDF)
    val result = model.transform(mlTokenDF)

    val vectorResults = result.select("VectorResult")
    val vectors = vectorResults.map { x => x.getAs[Vector](0) }
    val kMeansModel = KMeans.train(vectors, 8, 20)
    val predictions = result.map { r => (r.getString(1), kMeansModel.predict(r.getAs[Vector](3))) }
    val predictionDF = predictions.toDF("source_recordID", "ClusterID")

    //predictionDF.insertInto("adv_analytics.ca_cluster001")
    predictionDF.write.mode("append").saveAsTable(cobraClusterTable);
    LOG.info(s"""predictionDF data has been loaded into $cobraClusterTable""");
//=====================================================
    val masterDateCastDF = masterDF.withColumn("tweettime", col("tweettime").cast("date"))

    val wordCountBaseDF = masterDateCastDF.select("tweettime", "TweetCleanedMessage", "bell_flag_id", "rogers_flag_id", "freedom_flag_id", "telus_flag_id", "chatr_flag_id", "fido_flag_id", "kodoo_flag_id", "public_flag_id", "shaw_flag_id", "virgin_flag_id", "bbw_flag_id", "master_word")
      .filter($"master_word" !== "others")
      .explode("TweetCleanedMessage", "TweetWords")((line: String) => line.split(" "))

    //val wordsDF = hiveContext.sql("select cast(to_date(tweettime) as timestamp) as tweettime,TweetCleanedMessage,bell_flag_id,rogers_flag_id,freedom_flag_id,telus_flag_id,chatr_flag_id,fido_flag_id,kodoo_flag_id,public_flag_id,shaw_flag_id,virgin_flag_id,bbw_flag_id from adv_analytics.ca_master where master_word !='others'").explode("TweetCleanedMessage", "TweetWords")((line: String) => line.split(" "))

    val overallWordCountDF = wordCountBaseDF.groupBy("tweettime", "TweetWords").count().withColumn("master_word", lit("overall"))
    //overallWordCountDF.insertInto("adv_analytics.ca_wordcount001")
    overallWordCountDF.write.mode("append").saveAsTable(cobraWordCountTable);
    LOG.info(s"""overallWordCountDF data has been loaded into $cobraClusterTable""");

    val bellWordCountDF = wordCountBaseDF.filter($"bell_flag_id" === 1).groupBy("tweettime", "TweetWords").count().withColumn("master_word", lit("bell"))
    //bellWordCountDF.insertInto("adv_analytics.ca_wordcount001")
    bellWordCountDF.write.mode("append").saveAsTable(cobraWordCountTable);
    LOG.info(s"""bellWordCountDF data has been loaded into $cobraClusterTable""");

    val rogersWordCountDF = wordCountBaseDF.filter($"rogers_flag_id" === 1).groupBy("tweettime", "TweetWords").count().withColumn("master_word", lit("rogers"))
    //rogersWordCountDF.insertInto("adv_analytics.ca_wordcount001")
    rogersWordCountDF.write.mode("append").saveAsTable(cobraWordCountTable);
    LOG.info(s"""rogersWordCountDF data has been loaded into $cobraClusterTable""");

    val freedomWordCountDF = wordCountBaseDF.filter($"freedom_flag_id" === 1).groupBy("tweettime", "TweetWords").count().withColumn("master_word", lit("freedom"))
    //freedomWordCountDF.insertInto("adv_analytics.ca_wordcount001")
    freedomWordCountDF.write.mode("append").saveAsTable(cobraWordCountTable);
    LOG.info(s"""freedomWordCountDF data has been loaded into $cobraClusterTable""");

    val telusWordCountDF = wordCountBaseDF.filter($"telus_flag_id" === 1).groupBy("tweettime", "TweetWords").count().withColumn("master_word", lit("telus"))
    //telusWordCountDF.insertInto("adv_analytics.ca_wordcount001")
    telusWordCountDF.write.mode("append").saveAsTable(cobraWordCountTable);
    LOG.info(s"""telusWordCountDF data has been loaded into $cobraClusterTable""");

    val chatrWordCountDF = wordCountBaseDF.filter($"chatr_flag_id" === 1).groupBy("tweettime", "TweetWords").count().withColumn("master_word", lit("chatr"))
    //chatrWordCountDF.insertInto("adv_analytics.ca_wordcount001")
    chatrWordCountDF.write.mode("append").saveAsTable(cobraWordCountTable);
    LOG.info(s"""chatrWordCountDF data has been loaded into $cobraClusterTable""");

    val fidoWordCountDF = wordCountBaseDF.filter($"fido_flag_id" === 1).groupBy("tweettime", "TweetWords").count().withColumn("master_word", lit("fido"))
    //fidoWordCountDF.insertInto("adv_analytics.ca_wordcount001")
    fidoWordCountDF.write.mode("append").saveAsTable(cobraWordCountTable);
    LOG.info(s"""fidoWordCountDF data has been loaded into $cobraClusterTable""");

    val kodooWordCountDF = wordCountBaseDF.filter($"kodoo_flag_id" === 1).groupBy("tweettime", "TweetWords").count().withColumn("master_word", lit("koodo"))
    //kodooWordCountDF.insertInto("adv_analytics.ca_wordcount001")
    kodooWordCountDF.write.mode("append").saveAsTable(cobraWordCountTable);
    LOG.info(s"""kodooWordCountDF data has been loaded into $cobraClusterTable""");

    val publicWordCountDF = wordCountBaseDF.filter($"public_flag_id" === 1).groupBy("tweettime", "TweetWords").count().withColumn("master_word", lit("public"))
    //publicWordCountDF.insertInto("adv_analytics.ca_wordcount001")
    publicWordCountDF.write.mode("append").saveAsTable(cobraWordCountTable);
    LOG.info(s"""publicWordCountDF data has been loaded into $cobraClusterTable""");

    val shawWordCountDF = wordCountBaseDF.filter($"shaw_flag_id" === 1).groupBy("tweettime", "TweetWords").count().withColumn("master_word", lit("shaw"))
    //shawWordCountDF.insertInto("adv_analytics.ca_wordcount001")
    shawWordCountDF.write.mode("append").saveAsTable(cobraWordCountTable);
    LOG.info(s"""shawWordCountDF data has been loaded into $cobraClusterTable""");

    val virginWordCountDF = wordCountBaseDF.filter($"virgin_flag_id" === 1).groupBy("tweettime", "TweetWords").count().withColumn("master_word", lit("virgin"))
    //virginWordCountDF.insertInto("adv_analytics.ca_wordcount001")
    virginWordCountDF.write.mode("append").saveAsTable(cobraWordCountTable);
    LOG.info(s"""virginWordCountDF data has been loaded into $cobraClusterTable""");

    val bbwWordCountDF = wordCountBaseDF.filter($"bbw_flag_id" === 1).groupBy("tweettime", "TweetWords").count().withColumn("master_word", lit("bbw"))
    //bbwWordCountDF.insertInto("adv_analytics.ca_wordcount001")
    bbwWordCountDF.write.mode("append").saveAsTable(cobraWordCountTable);
    LOG.info(s"""bbwWordCountDF data has been loaded into $cobraClusterTable""");

  }
  
//  def addAdditionalColumns(lower:Int,upper:Int,df:DataFrame,hiveContext:HiveContext ):DataFrame={
//    
//    df.registerTempTable("getColumnsQuery_TEMP");
//    
//    val builder:StringBuilder = new StringBuilder();
//    for( i <- lower to upper ){
//      builder.append(s" '' as general_$i ,");
//    }
//    
//    var columns = builder.toString();
//    columns = df.schema.fields.map { x => x.name}.toArray.mkString(",")+ columns.substring(0, columns.lastIndexOf(","));
//    
//    val dfTmp = hiveContext.sql(s"""SELECT $columns FROM getColumnsQuery_TEMP""");
//    return dfTmp;
//  }

}