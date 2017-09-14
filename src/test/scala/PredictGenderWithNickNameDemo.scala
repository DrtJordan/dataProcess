import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yhao on 2017/9/14.
  */
object PredictGenderWithNickNameDemo extends Serializable {

  case class OrigData(userId: String, gender: Int, ageBucket: Int, nickName: String, appList: List[String],
                      playList: List[Long], collectList: List[Long], downloadList: List[Long], keywordList: List[String])

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val conf: SparkConf = new SparkConf().setAppName("get data").setMaster("local")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val filePath: String = "data/data_with_gender.dat"

    import spark.implicits._
    val data = sc.textFile(filePath).flatMap{line =>
      val tokens: Array[String] = line.split("\\|")

      if (tokens.length > 8) {
        val userId: String = tokens(0)
        val gender: Int = tokens(1).toInt
        val ageBucket: Int = tokens(2).toInt
        val nickName: String = tokens(3)
        val appList: List[String] = tokens(4).split(";").toList
        val playList: List[Long] = tokens(5).split(";").map(_.toLong).toList
        val collectList: List[Long] = tokens(6).split(";").map(_.toLong).toList
        val downloadList: List[Long] = tokens(7).split(";").map(_.toLong).toList
        val keyWordList: List[String] = tokens(8).split("#@#").toList

        Some(OrigData(userId, gender, ageBucket, nickName, appList, playList, collectList, downloadList, keyWordList))
      } else None
    }.toDF()



    spark.stop()
  }
}
