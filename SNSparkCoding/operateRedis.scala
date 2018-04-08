import org.apache.spark.sql.{SparkSession}
import redis.clients.jedis.Jedis

object operateRedis{
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("operateRedis").enableHiveSupport().getOrCreate()
    val dataDF = sparkSession.sql("SELECT distinct(utm_src_id),utm_src_cate_nm_1 FROM bi_td.tdpa_utm_src_td limit 10").cache()
    val resDF = dataDF.rdd.map(item=>(item(0).toString,item(1).toString))
    resDF.take(10).foreach(print)
    //val jedis :Jedis = new Jedis("10.27.119.171",6379)
    System.out.println("连接本地的 Redis 服务成功！")
    resDF.foreach(item=>Set2Redis(item))
  }
  def Set2Redis(data:Tuple2[String,String]) = {
    val jedis :Jedis = new Jedis("10.27.119.171",6379)
    jedis.set(data._1.toString,data._2.toString)
  }
}
