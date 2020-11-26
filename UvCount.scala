/**
 * get the unique ip addresses to access the same website
 *
 */


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object UvCount {
  def main(args: Array[String]): Unit = {
    //1、create the sparkconf object
    val sparkConf: SparkConf = new SparkConf().setAppName("PV").setMaster("local[2]")
    //2、create sparkcontext object
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")

    val dataRDD:RDD[String] = sc.textFile(this.getClass.getClassLoader.getResource("access.log").getPath)
    # get all the IP address
    val ipRDD: RDD[String] = dataRDD.map(x => x.split(" ")(0))
    val access_count:RDD[String] = ipRDD.distinct()

    val uv:String = access_count.count()
    println(s"uv:$uv")
    sc.stop(0)

  }
}