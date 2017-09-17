import com.kugou.mining.featureEngeering.FeatureSelector
import com.kugou.mining.preprocess.Preprocessor
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * Created by yhao on 2017/9/15 18:24.
  */
object SelectFeatureByChiSqDemo extends Serializable {
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Predict Gender With LR").setMaster("local")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val filePath = "G:/projectData/dataProcess_data/orig_data_with_age.dat"

    val text: RDD[String] = sc.textFile(filePath)
    val preprocessor = new Preprocessor
    val cleanedData = preprocessor.getAndCleanUserData(text)

    val discretedData: DataFrame = discretization(cleanedData, spark)

    val selector = new FeatureSelector()
      .setFeatureCol("features")
      .setLabelCol("label")
      .setFeatureNum(2)
      .setOutputCol("selectedFeatures")

    val selectedDF: DataFrame = selector.selectWithChiSq(discretedData)

    spark.stop()
  }


  def discretization(data: RDD[Array[Double]], spark: SparkSession): DataFrame = {
    val schemaString = new Array[String](41)
    schemaString(0) = "label"
    for (i <- 1 to 40) schemaString(i) = "feature" + i.toString
    val schema = StructType(schemaString.map(fieldName => StructField(fieldName, DoubleType, nullable = true)))

    val cleanedDataRow: RDD[Row] = data.map(record => Row.fromSeq(record))
    val cleanedDataDF: DataFrame = spark.createDataFrame(cleanedDataRow, schema)

    val label: DataFrame = cleanedDataDF.select("label")
    var zipResult: DataFrame = label

    for (k <- 1 to 40) {
      val param1: String = "feature" + k.toString
      val param2: String = "feature" + k.toString + "Dis"

      val varDis: DataFrame = fieldDiscretization(param1, param2, cleanedDataDF)
      val zipedRDD: RDD[Row] = zipResult.rdd.repartition(1).zip(varDis.rdd.repartition(1))
        .map(record => Row.fromSeq(record._1.toSeq ++ record._2.toSeq))

      val zipedSchema = StructType(zipResult.schema ++ varDis.schema)
      zipResult = spark.createDataFrame(zipedRDD, zipedSchema)
    }

    val outputRDD: RDD[(Double, Vector)] = zipResult.rdd.map{row =>
      val tokens: Array[Double] = row.toSeq.toArray.map(x => x.toString.toDouble)
      val label: Double = tokens(0)
      val featuresArray: Array[Double] = tokens.drop(1)
      (label, Vectors.dense(featuresArray))
    }

    import spark.implicits._
    outputRDD.toDF("label", "features")
  }


  def fieldDiscretization(inputCol:String, outputCol:String, rowDataFrame: DataFrame):DataFrame = { //定制的特征离散化函数
    val numVar = rowDataFrame.select(inputCol).rdd.map(k => k(0).toString.toDouble).collect()
    val dataFrameNumVar = rowDataFrame.sparkSession.createDataFrame(numVar.map(Tuple1.apply)).toDF("features")

    val min: Double = numVar.min
    val max: Double = numVar.max

    //切分点数组
    val splits = Array(
      min,
      (max - min) * 0.12,
      (max - min) * 0.15,
      (max - min) * 0.18,
      (max - min) * 0.22,
      (max - min) * 0.27,
      (max - min) * 0.32,
      (max - min) * 0.37,
      (max - min) * 0.42,
      (max - min) * 0.47,
      max)

    //将特征离散化
    val bucketizer = new Bucketizer()
      .setInputCol("features")
      .setOutputCol("bucketedFeatures")
      .setSplits(splits)
    val bucketedData = bucketizer.transform(dataFrameNumVar)

    //对离散化之后的特征列进行命名
    val DisVar = bucketedData.select("bucketedFeatures").withColumnRenamed("bucketedFeatures", outputCol)
    DisVar
  }
}
