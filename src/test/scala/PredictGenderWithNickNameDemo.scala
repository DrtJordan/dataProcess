import com.kugou.mining.algorithms.MultiClassifications
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, StopWordsRemover}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object PredictGenderWithNickNameDemo extends Serializable {
  val logger: Logger = Logger.getLogger(this.getClass)

  case class OrigData(userId: String, gender: Double, ageBucket: Int, nickName: String, nickWordList: List[String],
                      appList: List[String], playList: List[Long])

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val conf: SparkConf = new SparkConf().setAppName("Predict Gender With LR").setMaster("local")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val filePath = "data/data_with_gender.dat"
    val stopwordPath: String = "dictionaries/stopwords.txt"

    //数据DataFrame
    import spark.implicits._
    val data = sc.textFile(filePath).flatMap{line =>
      val tokens: Array[String] = line.split("\\|")

      if (tokens.length > 8) {
        val userId: String = tokens(0)
        val gender: Double = tokens(1).toDouble
        val ageBucket: Int = tokens(2).toInt
        val nickName: String = tokens(3)
        val nickWordList: List[String] = nickName.split("").toList
        val appList: List[String] = tokens(4).split(";").toList
        val playList: List[Long] = tokens(5).split(";").filter(_.nonEmpty).map(_.toLong).toList

        Some(OrigData(userId, gender, ageBucket, nickName, nickWordList, appList, playList))
      } else None
    }.toDF()

    //去除停用词
    val stopwords: Array[String] = sc.textFile(stopwordPath).collect()
    val remover = new StopWordsRemover()
      .setInputCol("nickWordList")
      .setOutputCol("filterNickList")
      .setStopWords(stopwords)
    val removedData: DataFrame = remover.transform(data)

    //向量化(使用词频为单元)
    val vectorizer = new CountVectorizer()
      .setInputCol("filterNickList")
      .setOutputCol("features")
      .setVocabSize(20000)
    val cvModel: CountVectorizerModel = vectorizer.fit(removedData)
    val vectorizeData: DataFrame = cvModel.transform(removedData)

    //使用LR进行训练和预测
    val Array(trainData: Dataset[Row], testData: Dataset[Row]) = vectorizeData.randomSplit(Array(0.7, 0.3))
    val classifications = new MultiClassifications(spark)
      .setFeatureCol("features")
      .setLabelCol("gender")
      .setPredictionCol("predictions")
      .setMethod("LR")
    val model: LogisticRegressionModel = classifications.trainWithLR(trainData)     //训练LR模型
    val predictData: DataFrame = classifications.predictWithLR(testData, model)     //使用LR模型进行预测
    
    //对模型进行评估
    val evaluateData: RDD[(Double, Double)] = predictData.select("predictions", "gender").rdd
      .map{case Row(prediction: Double, gender: Double) => (prediction, gender)}
    val (accuracy, (precision, recall, f1)) = classifications.evaluateWithLR(evaluateData)

    logger.info("模型评估结果：")
    logger.info(s"\t准确率：$accuracy \n")
    logger.info(s"\t精确率：$precision")
    logger.info(s"\t召回率：$recall")
    logger.info(s"\tF1-Measure：$f1")

    spark.stop()
  }
}
