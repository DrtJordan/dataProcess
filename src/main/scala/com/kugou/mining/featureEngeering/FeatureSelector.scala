package com.kugou.mining.featureEngeering

import org.apache.log4j.Logger
import org.apache.spark.ml.feature.{ChiSqSelector, ChiSqSelectorModel}
import org.apache.spark.sql.DataFrame

/**
  *
  * Created by yhao on 2017/9/15 13:48.
  */
class FeatureSelector extends Serializable {
  val logger = Logger.getLogger(this.getClass)

  private var featureNum: Int = 1
  private var featureCol: String = "features"
  private var labelCol: String = "labels"
  private var outputCol: String = "selectedFeatures"

  def setFeatureNum(value: Int): this.type = {
    this.featureNum = value
    this
  }

  def setFeatureCol(value: String): this.type = {
    this.featureCol = value
    this
  }

  def setLabelCol(value: String): this.type = {
    this.labelCol = value
    this
  }

  def setOutputCol(value: String): this.type = {
    this.outputCol = value
    this
  }


  /**
    * 使用卡方检验ChiSq进行特征选择
    *
    * @param data   待选择特征
    * @return   选择后特征
    */
  def selectWithChiSq(data: DataFrame): DataFrame = {
    val selector = new ChiSqSelector()
      .setNumTopFeatures(featureNum)
      .setFeaturesCol(featureCol)
      .setLabelCol(labelCol)
      .setOutputCol(outputCol)

    val chiSqModel: ChiSqSelectorModel = selector.fit(data)
    chiSqModel.transform(data)
  }
}
