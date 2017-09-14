import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.junit.Test


/**
  * 预处理测试类
  *
  * @author   yhao
  */
class Preprocess {
  Logger.getLogger("org").setLevel(Level.WARN)

  @Test
  def getData(): Unit ={
    val conf: SparkConf = new SparkConf().setAppName("get data").setMaster("local")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val filePath: String = "F:/data/001000_0"
    val outputPath: String = "F:/data/data_with_ gender.dat"
    val data: RDD[String] = sc.textFile(filePath)

    val filterData = data.flatMap{line =>
      val tokens: Array[String] = line.split("\\|").map(_.replaceAll("\\\\N", ""))
      if (tokens.length > 8) {
        val userId: String = tokens(0).trim     //用户ID
        val gender: Int = if (tokens(1).trim.isEmpty) -1 else tokens(1).trim.toInt     //用户性别（男0|女1）
        val ageBracket: Int = if (tokens(2).trim.isEmpty) -1 else tokens(2).trim.toInt       //年龄段
        val nickName: String = tokens(3).trim     //用户昵称
        val appList: Array[String] = tokens(4).split(";").map(_.trim)     //使用app列表
        val playList: Array[String] = tokens(5).split(";").map(_.split(":")(0))     //播放歌曲列表
        val collectList: Array[String] = tokens(6).split(";").map(_.trim)        //收藏歌曲列表
        val downloadList: Array[String] = tokens(7).split(";").map(_.trim)       //下载歌曲列表
        val searchKeyWordList: Array[String] = tokens(8).split("#@#").map(_.trim)      //搜索关键词列表

        Some(userId, gender, ageBracket, nickName, appList, playList, collectList, downloadList, searchKeyWordList)
      } else None
    }.filter(_._2 >= 0)

    val result = filterData.map{record =>
      val arr: Array[String] = Array(record._1, record._2.toString, record._3.toString, record._4, record._5.mkString(";"),
          record._6.mkString(";"), record._7.mkString(";"), record._8.mkString(";"), record._9.mkString("#@#"))
      arr.mkString("|")
    }

    val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath), "utf-8"))
    result.collect().foreach(line => bw.write(line + "\n"))
    bw.close()

    spark.stop()
  }
}
