import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object VisitTopN {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("VisitTopN").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")

    val dataRDD:RDD[String] = sc.textFile(this.getClass().getClassLoader.getResource("access.log").getPath)
    // ================ try =============
    // 分割数据
    /*
    val arrayRDD:RDD[String] = dataRDD.map(x => (x.split(" ")))
    // 读取所需列数据
    val urlRDD:RDD[String] = arrayRDD[10]
    // 统计前五的url --- 下标是10
    val OrderRDD:RDD[(String, Int)] = urlRDD.xxx
    // ================ try =============
    */
    // filter the log with length above 10
    val arrayRDD:RDD[String] = dataRDD.filter(x => (x.split(" ").length>10))
    // extract the 10th value
    val urlsRDD:RDD[String] = arrayRDD.map(x => x.split(" ")(10))
    // filter the logs with the http request
    val httpRDD:RDD[String] = urlsRDD.filter(x => x.contains("http"))
    // convert string to (string, int)
    val urlArrayRDD:RDD[(String, Int)] = httpRDD.map(x => (x,1))
    // aggregate the number
    val aggUrls:RDD[(String,Int)] = urlArrayRDD.reduceByKey(_+_)
    // sort by number
    val orderUrls:RDD[(String, Int)] = aggUrls.sortBy(_._2,false)
    // extract the top5
    val topUrls:Array[(String,Int)] = orderUrls.take(5)
  }
}