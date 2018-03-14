import com.huaban.analysis.jieba.JiebaSegmenter
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
object WordSp {
  def main(args: Array[String]): Unit = {
    //val conf = new SparkConf().setAppName("input").setMaster("local[*]")
    //val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder().appName("sparktest").enableHiveSupport().getOrCreate()
    val rdd=sparkSession.sparkContext.textFile("hdfs://10.27.1.138:9000/user/sopdm/ceshi.txt")
    //val nameDF = sparkSession.sql("SELECT gds_nm FROM BI_DM.DM_GDS_INF_TD where gds_cd = 143026114").cache()
    val nameDF = sparkSession.sql("SELECT gds_nm FROM BI_DM.DM_GDS_INF_TD").cache()
    nameDF.show()
    //val strarr = ArrayBuffer[String]()
    //rdd.foreach(println)
    //nameDF.rdd.map(item=>)
    //nameDF.collect().foreach(strarr+=_.toString())
    //strarr.foreach(println)
    /*strarr.map{ x =>
      var str = if (x.length > 0)
        new JiebaSegmenter().sentenceProcess(x)
      str.toString
    }.foreach(println)*/
    val chars = nameDF.rdd.map(item=>getStrArr(item(0).toString.trim))
    chars.foreach{item=> item.foreach(item=>print(item+" "))
      println()}
    /*val chars = nameDF.rdd.map{ x =>
      var str = if (x(0).toString().length > 0)
        new JiebaSegmenter().sentenceProcess(x(0).toString())
    }*/
    /*rdd.map { x =>
      var str = if (x.length > 0)
        new JiebaSegmenter().sentenceProcess(x)
      str.toString
    }.foreach(println)*/
  }

  def getStrArr(str:String):Array[String]= {
    //val regEx = "[`~!@#$%^&*()+=|{}':;',\\[\\].<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。，、？×《》-]".r
    //val regEx = "[^\u4e00-\u9fa5][^0-9][^a-zA-Z]+".r
    val regEx="[^0-9a-zA-Z\u4e00-\u9fa5]+".r
    val arr = ArrayBuffer[String]()
    val list = new JiebaSegmenter().sentenceProcess(str.trim)
    list.toArray().map(item=>regEx.replaceAllIn(item.toString," ")).filter(_!=" ").foreach(item=>if(item!=" ")arr+=item.toString.trim)
    //val list = str.trim.split(" ").toList
    //list.map(item=>regEx.replaceAllIn(item.toString," ")).filter(_!=" ").foreach(arr+=_.toString)
    arr.toArray
  }
}
