object Test {
  def main(args: Array[String]): Unit = {
    val text = "sdfhrtrmndf\\Ndfsdjkljfe"

    println(text.replaceAll("\\\\N", " "))
  }
}
