object Test {
  def main(args: Array[String]): Unit = {
    val text = "我是中国人"

    val strArr: Array[String] = text.split("")

    strArr.foreach(println)
  }
}
