import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object PredictGenderWithLRDemo extends Serializable {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val conf: SparkConf = new SparkConf().setAppName("Predict Gender With LR").setMaster("local")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val filePath = "data/data_with_ gender.dat"

    val data = sc.textFile(filePath).flatMap{line =>
      val tokens: Array[String] = line.split("\\|")

      if (tokens.length > 8) {
        val userId: String = tokens(0)
        val gender: Int = tokens(1).toInt
        val nickName: String = tokens(3)
        val appList: List[String] = tokens(4).split(";").toList
        val playList: List[String] = tokens(5).split(";").toList
        Some(userId, gender, nickName, appList, playList)
      } else None
    }
  }
}
