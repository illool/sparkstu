import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object JdbcDbAccess {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("JdbcDbAccess")
    val sc = new SparkContext(conf)
    //SQLContext被遗弃的方法，SparkSession
    //val sqlContext = new SQLContext(sc)现在推荐使用
    val sparkSession = SparkSession.builder().appName("sparktest").enableHiveSupport().getOrCreate()
    val sqlDF = sparkSession.sql("SELECT * FROM SOPDM.TDM_ML_OR_ORDER_D")
    val sqlDF1 = sqlDF.filter("PAY_TIME IS NOT NULL")
    //sqlDF1.printSchema()
    //sqlDF1.show(10)
    //sqlDF.groupBy("GDS_ID").max("SHLD_AMNT").show()
    val cou = sqlDF1.groupBy("GDS_ID").count.as("count")
    val max = sqlDF1.groupBy("GDS_ID").max("SHLD_AMNT").as("max")
    val avg = sqlDF1.groupBy("GDS_ID").avg("SHLD_AMNT").as("avg")
    val min = sqlDF1.groupBy("GDS_ID").min("SHLD_AMNT").as("min")
    val res = cou.join(max,cou("GDS_ID") === max("GDS_ID"))
    val testrow = sqlDF1.rdd.map(t => (t(0), 1)).reduceByKey((x,y) => x + y)
    val testrow2 = sqlDF1.rdd.map(t => (t(0), 1)).countByKey().toSeq
    val testrow3 = sqlDF1.rdd.map(t => Row(t(0),t(1),t(2)))
    testrow3.collect().foreach(println)
    //testrow.collect().foreach(println)
    //testrow2.foreach(println)
    //val testrow = sqlDF1.map(t => "GDS_ID:" + t(0)).collect().foreach(println)
    val row = Row(1, true, "a string", null)
    val firstValue = row(0)
    println(firstValue)
    val fourthValue = row(3)
    println(fourthValue)
    //res.show()
    //sqlDF1.describe().show()
    /*val jdbcDF = sparkSession.read
      .format("jdbc")
      .option("url", "jdbc:hive2://10.19.219.44:10002/sopdm?user=sopdm&password=sopdm@suning")
      .option("dbtable", "bi_td.tdm_ca_prmtr_td")
      .option("driver", "org.apache.hadoop.hive.jdbc.HiveDriver")
      .load()
    jdbcDF.select().show()*/
  }
}
