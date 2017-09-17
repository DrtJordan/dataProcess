package com.kugou.mining.preprocess

import org.apache.log4j.Logger
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

/**
  *
  * Created by yhao on 2017/9/15 16:48.
  */
class Preprocessor extends Serializable {
  val logger = Logger.getLogger(this.getClass)


  /**
    * 获取宽表中的字段
    *
    * @param data
    * @return
    */
  def getAndCleanUserData(data: RDD[String]) = {
    val result = data.flatMap{line =>
      val tokens: Array[String] = line.split("\\|").map(_.trim.replaceAll("\\\\N", ""))
      var count: Int = 0
      if (tokens.length > 20) {
        val age: Double = if (tokens(3).isEmpty) 10.0 else tokens(3).toDouble
        val cnt_start: Double = if (tokens(8).isEmpty) {count += 1; -1.0} else tokens(8).toDouble                    //启动次数
        val day_start: Double = if (tokens(9).isEmpty) {count += 1; -1.0} else tokens(9).toDouble                    //启动天数
        val cnt_login: Double = if (tokens(10).isEmpty) {count += 1; -1.0} else tokens(10).toDouble                   //登录次数
        val days_login: Double = if (tokens(11).isEmpty) {count += 1; -1.0} else tokens(11).toDouble                  //登录天数
        val days_play_audio: Double = if (tokens(12).isEmpty) {count += 1; -1.0} else tokens(12).toDouble             //音频播放天数
        val cnt_play_audio: Double = if (tokens(13).isEmpty) {count += 1; -1.0} else tokens(13).toDouble              //音频播放次数
        val dur_play_audio: Double = if (tokens(14).isEmpty) {count += 1; -1.0} else tokens(14).toDouble              //音频播放时长
        val cnt_scid: Double = if (tokens(15).isEmpty) {count += 1; -1.0} else tokens(15).toDouble                    //当月播放歌曲数
        val cnt_play_audio_lib: Double = if (tokens(16).isEmpty) {count += 1; -1.0} else tokens(16).toDouble                  //乐库音频播放次数
        val cnt_play_audio_radio: Double = if (tokens(17).isEmpty) {count += 1; -1.0} else tokens(17).toDouble                //电台音频播放次数
        val cnt_play_audio_search: Double = if (tokens(18).isEmpty) {count += 1; -1.0} else tokens(18).toDouble               //搜索音频播放次数
        val cnt_play_audio_local: Double = if (tokens(19).isEmpty) {count += 1; -1.0} else tokens(19).toDouble                //本地音频播放次数
        val cnt_play_audio_favorite: Double = if (tokens(20).isEmpty) {count += 1; -1.0} else tokens(20).toDouble             //网络收藏音频播放次数
        val cnt_play_audio_list: Double = if (tokens(21).isEmpty) {count += 1; -1.0} else tokens(21).toDouble                 //歌单音频播放次数
        val cnt_play_audio_like: Double = if (tokens(22).isEmpty) {count += 1; -1.0} else tokens(22).toDouble                 //猜你喜欢音频播放次数
        val cnt_play_audio_recommend: Double = if (tokens(23).isEmpty) {count += 1; -1.0} else tokens(23).toDouble            //每日推荐音频播放次数
        val cnt_play_video: Double = if (tokens(24).isEmpty) {count += 1; -1.0} else tokens(24).toDouble                      //视频播放次数
        val dur_play_video: Double = if (tokens(25).isEmpty) {count += 1; -1.0} else tokens(25).toDouble                      //视频播放时长
        val cnt_download_audio: Double = if (tokens(28).isEmpty) {count += 1; -1.0} else tokens(28).toDouble                  //音频下载次数
        val cnt_search: Double = if (tokens(29).isEmpty) {count += 1; -1.0} else tokens(29).toDouble                  //搜索次数
        val cnt_search_valid: Double = if (tokens(30).isEmpty) {count += 1; -1.0} else tokens(30).toDouble                    //有效搜索次数
        val cnt_favorite_scid: Double = if (tokens(31).isEmpty) {count += 1; -1.0} else tokens(31).toDouble                   //当月收藏歌曲数
        val cnt_favorite_list: Double = if (tokens(32).isEmpty) {count += 1; -1.0} else tokens(32).toDouble                   //当月收藏歌单数
        val cnt_share: Double = if (tokens(33).isEmpty) {count += 1; -1.0} else tokens(33).toDouble                   //当月分享次数
        val cnt_comment: Double = if (tokens(34).isEmpty) {count += 1; -1.0} else tokens(34).toDouble                 //当月评论次数
        val usr_follow: Double = if (tokens(35).isEmpty) {count += 1; -1.0} else tokens(35).toDouble                  //关注人数
        val usr_friend: Double = if (tokens(36).isEmpty) {count += 1; -1.0} else tokens(36).toDouble                  //好友人数
        val usr_fan: Double = if (tokens(37).isEmpty) {count += 1; -1.0} else tokens(37).toDouble                     //粉丝人数
        val dur_holiday: Double = if (tokens(40).isEmpty) {count += 1; -1.0} else tokens(40).toDouble                 //节假日播放时长
        val dur_weekday: Double = if (tokens(41).isEmpty) {count += 1; -1.0} else tokens(41).toDouble                 //工作日播放时长
        val pay_frequency: Double = if (tokens(48).isEmpty) {count += 1; -1.0} else tokens(48).toDouble                       //付费频次
        val year_musicpack_buy_cnt: Double = if (tokens(52).isEmpty) {count += 1; -1.0} else tokens(52).toDouble              //音乐包-本年度购买次数
        val year_musicpack_free_cnt: Double = if (tokens(54).isEmpty) {count += 1; -1.0} else tokens(54).toDouble             //音乐包-本年度赠送次数
        val musicpack_auto_buy_cnt: Double = if (tokens(57).isEmpty) {count += 1; -1.0} else tokens(57).toDouble              //音乐包-自动续费次数
        val last_musicpack_buy_apart_days: Double = if (tokens(62).isEmpty) {count += 1; -1.0} else tokens(62).toDouble               //音乐包-和上次购买相隔天数
        val year_svip_buy_cnt: Double = if (tokens(65).isEmpty) {count += 1; -1.0} else tokens(65).toDouble                   //SVIP-本年度购买次数
        val single_buy_cnt: Double = if (tokens(70).isEmpty) {count += 1; -1.0} else tokens(70).toDouble                      //单曲-购买总数
        val single_buy_times: Double = if (tokens(71).isEmpty) {count += 1; -1.0} else tokens(71).toDouble                    //单曲-购买次数
        val year_album_buy_cnt: Double = if (tokens(74).isEmpty) {count += 1; -1.0} else tokens(74).toDouble                  //专辑-今年购买总量
        val year_album_buy_times: Double = if (tokens(75).isEmpty) {count += 1; -1.0} else tokens(75).toDouble                //专辑-今年购买次数

        val rate: Double = 1.0 * count / 41

        if (rate < 0.5) {
          Some(Array(age, cnt_start, day_start, cnt_login, days_login, days_play_audio, cnt_play_audio,
            dur_play_audio, cnt_scid, cnt_play_audio_lib, cnt_play_audio_radio, cnt_play_audio_search,
            cnt_play_audio_local, cnt_play_audio_favorite, cnt_play_audio_list, cnt_play_audio_like,
            cnt_play_audio_recommend, cnt_play_video, dur_play_video, cnt_download_audio, cnt_search, cnt_search_valid,
            cnt_favorite_scid, cnt_favorite_list, cnt_share, cnt_comment, usr_follow, usr_friend, usr_fan, dur_holiday,
            dur_weekday, pay_frequency, year_musicpack_buy_cnt, year_musicpack_free_cnt,
            musicpack_auto_buy_cnt, last_musicpack_buy_apart_days, year_svip_buy_cnt, single_buy_cnt, single_buy_times,
            year_album_buy_cnt, year_album_buy_times))
        } else None
      } else None
    }

    result
  }
}
