import com.kugou.mining.featureEngeering.FeatureSelector
import com.kugou.mining.preprocess.Preprocessor
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * Created by yhao on 2017/9/15 18:24.
  */
object SelectFeatureByChiSqDemo extends Serializable {
  Logger.getLogger("org").setLevel(Level.WARN)

  case class Cleaned(features: linalg.Vector, label: Double)

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Predict Gender With LR").setMaster("local")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val filePath = "F:/data/000000_0"

    val text: RDD[String] = sc.textFile(filePath)
    val preprocessor = new Preprocessor
    val cleanedData: RDD[(linalg.Vector, Double)] = preprocessor.getAndCleanUserData(text)

    import spark.implicits._
    val cleanedDF: DataFrame = cleanedData.map{case (features: linalg.Vector, age: Double) => Cleaned(features, age)}.toDF()

    val selector = new FeatureSelector()
      .setFeatureCol("features")
      .setLabelCol("label")
  }
}
