package com.kugou.mining.algorithms

import org.apache.log4j.Logger
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * 多分类训练与测试工具类
  *
  * @param spark    SparkSession
  * @author   yhao
  */
class MultiClassifications(val spark: SparkSession) extends Serializable {
  val logger = Logger.getLogger(this.getClass)

  private var method: String = "LR"
  private var iterNum: Int = 100
  private var regParam: Double = 0.1
  private var aggregationDepth: Int = 2         //聚合深度，建议不小于2，如果特征较多，或者分区较多时可适当增大该值
  private var elasticNetParam: Double = 0.0
  private var tol: Double = 1E-6


  def setMethod(value: String): this.type = {
    this.method = value
    this
  }

  def setIterNum(value: Int): this.type = {
    this.iterNum = value
    this
  }

  def setRegParam(value: Double): this.type = {
    this.regParam = value
    this
  }

  def setAggregationDepth(value: Int): this.type = {
    this.aggregationDepth = value
    this
  }

  def setElasticNetParam(value: Double): this.type = {
    this.elasticNetParam = value
    this
  }

  def setTol(value: Double): this.type = {
    this.tol = value
    this
  }


  def train(data: DataFrame) = {
    val model = method match {
      case "LR" => trainWithLR(data)
      case _ =>
    }

    model
  }


  /**
    * 使用LR模型进行训练
    *
    * @param data   待训练数据
    * @return   LR模型
    */
  def trainWithLR(data: DataFrame): LogisticRegressionModel = {
    val lr = new LogisticRegression()
      .setMaxIter(iterNum)
      .setRegParam(regParam)
      .setAggregationDepth(aggregationDepth)
      .setElasticNetParam(elasticNetParam)
      .setTol(tol)

    logger.info("模型参数：")
    logger.info(s"\t最大迭代次数：$iterNum")
    logger.info(s"\t正则化参数：$regParam")
    logger.info(s"\t聚合深度：$aggregationDepth")
    logger.info(s"\tL1:L2正则比例：$elasticNetParam")
    logger.info(s"\t收敛阈值：$tol")

    logger.info("开始训练模型...")
    val startTime: Long = System.currentTimeMillis()
    val model: LogisticRegressionModel = lr.fit(data)
    val totalTime: Double = (System.currentTimeMillis() - startTime) / 1e3
    logger.info(s"模型训练完成！耗时：$totalTime sec")

    model
  }


  /**
    * 使用LR模型进行预测
    *
    * @param data     待测试数据
    * @param model    LR模型
    * @return   预测结果
    */
  def predictWithLR(data: DataFrame, model: LogisticRegressionModel): DataFrame = {
    logger.info("开始预测数据...")
    val startTime: Long = System.currentTimeMillis()
    val predictions: DataFrame = model.transform(data)
    val totalTime: Double = (System.currentTimeMillis() - startTime) / 1e3
    logger.info(s"预测数据完成！耗时：$totalTime sec")

    predictions
  }


  /**
    * LR模型预测结果评估
    *
    * @param predictionAndLabels    LR模型预测结果，包括: (预测结果、标签)
    * @return   (准确率, (F1值, 精确率, 召回率))
    */
  def evaluateWithLR(predictionAndLabels: RDD[(Double, Double)]): (Double, (Double, Double, Double)) = {
    val metrics = new MulticlassMetrics(predictionAndLabels)

    val f1 = metrics.weightedFMeasure
    val weightedPrecision = metrics.weightedPrecision
    val weightedRecall = metrics.weightedRecall
    val accuracy = metrics.accuracy

    (accuracy, (f1, weightedPrecision, weightedRecall))
  }
}
