import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.{Vectors, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.junit.Test


/**
  * 预处理测试类
  *
  * @author   yhao
  */
class PreprocessTest {
  Logger.getLogger("org").setLevel(Level.WARN)

  @Test
  def getData(): Unit ={
    val conf: SparkConf = new SparkConf().setAppName("get data").setMaster("local")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val filePath: String = "F:/data/001000_0"
    val outputPath: String = "F:/data/data_with_gender.dat"
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


  @Test
  def getAndCleanUserData(): RDD[(Vector, Double)] ={
    val conf: SparkConf = new SparkConf().setAppName("get data").setMaster("local")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val filePath: String = ""
    val outputPath: String = "F:/data/user_data_cleaned.dat"
    val data = sc.textFile(filePath).flatMap{line =>
      val tokens: Array[String] = line.split("\\|")
      if (tokens.length > 20) {
        val age: Double = tokens(3).toDouble
        val cnt_start: Double = tokens(8).toDouble                    //启动次数
        val day_start: Double = tokens(9).toDouble                    //启动天数
        val cnt_login: Double = tokens(10).toDouble                   //登录次数
        val days_login: Double = tokens(11).toDouble                  //登录天数
        val days_play_audio: Double = tokens(12).toDouble             //音频播放天数
        val cnt_play_audio: Double = tokens(13).toDouble              //音频播放次数
        val dur_play_audio: Double = tokens(14).toDouble              //音频播放时长
        val cnt_scid: Double = tokens(15).toDouble                    //当月播放歌曲数
        val cnt_play_audio_lib: Double = tokens(16).toDouble                  //乐库音频播放次数
        val cnt_play_audio_radio: Double = tokens(17).toDouble                //电台音频播放次数
        val cnt_play_audio_search: Double = tokens(18).toDouble               //搜索音频播放次数
        val cnt_play_audio_local: Double = tokens(19).toDouble                //本地音频播放次数
        val cnt_play_audio_favorite: Double = tokens(20).toDouble             //网络收藏音频播放次数
        val cnt_play_audio_list: Double = tokens(21).toDouble                 //歌单音频播放次数
        val cnt_play_audio_like: Double = tokens(22).toDouble                 //猜你喜欢音频播放次数
        val cnt_play_audio_recommend: Double = tokens(23).toDouble            //每日推荐音频播放次数
        val cnt_play_video: Double = tokens(24).toDouble                      //视频播放次数
        val dur_play_video: Double = tokens(25).toDouble                      //视频播放时长
        val cnt_download_audio: Double = tokens(28).toDouble                  //音频下载次数
        val cnt_search: Double = tokens(29).toDouble                  //搜索次数
        val cnt_search_valid: Double = tokens(30).toDouble                    //有效搜索次数
        val cnt_favorite_scid: Double = tokens(31).toDouble                   //当月收藏歌曲数
        val cnt_favorite_list: Double = tokens(32).toDouble                   //当月收藏歌单数
        val cnt_share: Double = tokens(33).toDouble                   //当月分享次数
        val cnt_comment: Double = tokens(34).toDouble                 //当月评论次数
        val usr_follow: Double = tokens(35).toDouble                  //关注人数
        val usr_friend: Double = tokens(36).toDouble                  //好友人数
        val usr_fan: Double = tokens(37).toDouble                     //粉丝人数
        val dur_holiday: Double = tokens(40).toDouble                 //节假日播放时长
        val dur_weekday: Double = tokens(41).toDouble                 //工作日播放时长
        val time_interval: Double = tokens(42).toDouble                       //时段偏好
        val pay_frequency: Double = tokens(48).toDouble                       //付费频次
        val year_musicpack_buy_cnt: Double = tokens(52).toDouble              //音乐包-本年度购买次数
        val year_musicpack_free_cnt: Double = tokens(54).toDouble             //音乐包-本年度赠送次数
        val musicpack_auto_buy_cnt: Double = tokens(57).toDouble              //音乐包-自动续费次数
        val last_musicpack_buy_apart_days: Double = tokens(62).toDouble               //音乐包-和上次购买相隔天数
        val year_svip_buy_cnt: Double = tokens(65).toDouble                   //SVIP-本年度购买次数
        val single_buy_cnt: Double = tokens(70).toDouble                      //单曲-购买总数
        val single_buy_times: Double = tokens(71).toDouble                    //单曲-购买次数
        val year_album_buy_cnt: Double = tokens(74).toDouble                  //专辑-今年购买总量
        val year_album_buy_times: Double = tokens(75).toDouble                //专辑-今年购买次数

        Some((Vectors.dense(Array(cnt_start, day_start, cnt_login, days_login, days_play_audio, cnt_play_audio,
          dur_play_audio, cnt_scid, cnt_play_audio_lib, cnt_play_audio_radio, cnt_play_audio_search,
          cnt_play_audio_local, cnt_play_audio_favorite, cnt_play_audio_list, cnt_play_audio_like,
          cnt_play_audio_recommend, cnt_play_video, dur_play_video, cnt_download_audio, cnt_search, cnt_search_valid,
          cnt_favorite_scid, cnt_favorite_list, cnt_share, cnt_comment, usr_follow, usr_friend, usr_fan, dur_holiday,
          dur_weekday, time_interval, pay_frequency, year_musicpack_buy_cnt, year_musicpack_free_cnt,
          musicpack_auto_buy_cnt, last_musicpack_buy_apart_days, year_svip_buy_cnt, single_buy_cnt, single_buy_times,
          year_album_buy_cnt, year_album_buy_times)), age))
      } else None
    }

    data
  }
}
