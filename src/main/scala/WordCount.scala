import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]): Unit = {
    val input = args(0)
    val output = args(1)
    val sc = new SparkContext()
    sc.textFile(input)
      .flatMap(line => line.split("\\s"))
      .map(word => (word, 1))
      .reduceByKey(_ + _) // .reduceByKey((x, y) => x + y)
      .sortBy { case (word, count) => count } // .sortBy(_._2) or .sortBy(pair => pair._2)
      .saveAsTextFile(output)
    sc.stop()
  }
}


//sbt package

//spark-submit --class WordCount --master yarn --deploy-mode cluster scala_spark_hadoop_rdd_2.11-0.1.jar
// /user/hduser/ppkm/ppkm_dataset.csv /user/hduser/ppkm-rdd-out

//docker cp target/scala-2.11/scala_spark_hadoop_rdd_2.11-0.1.jar gbhdp:/home/hduser/