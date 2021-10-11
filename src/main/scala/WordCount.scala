import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]): Unit = {
    val input = args(0)
    val output = args(1)
    val sc = new SparkContext()
    sc.textFile(input)
      .flatMap(line => line.trim().replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s"))
      .map(word => (word, 1))
      .reduceByKey(_ + _) // .reduceByKey((x, y) => x + y)
      .map { case (word, count) => (count, word) } // поменяли местами
      .sortBy { case (count, word) => count } // .sortBy(_._2) or .sortBy(pair => pair._2)
      .map{ case (word, count) => s"$word;$count" }
      .saveAsTextFile(output)
    sc.stop()
  }
}

//.toString.trim().replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s")

//sbt package

//spark-submit --class WordCount --master yarn --deploy-mode cluster scala_spark_hadoop_rdd_2.11-0.1.jar /user/hduser/ppkm/ppkm_dataset.csv /user/hduser/ppkm-rdd-out

//docker cp target/scala-2.11/scala_spark_hadoop_rdd_2.11-0.1.jar gbhdp:/home/hduser/

//wget https://apache-mirror.rbc.ru/pub/apache/spark/spark-2.4.8/spark-2.4.8-bin-hadoop2.7.tgz

//tar xzf spark-2.4.8-bin-hadoop2.7.tgz && rm spark-2.4.8-bin-hadoop2.7.tgz && mv spark-2.4.8-bin-hadoop2.7 spark

//export SPARK_HOME=/home/hduser/spark
//export PATH=$PATH:$SPARK_HOME/bin
//export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop/