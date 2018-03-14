import ResembleTest.calJaccard
import com.huaban.analysis.jieba.JiebaSegmenter
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer
object ResembleTest{// extends Serializable {
  private  val sparkSession = SparkSession.builder().appName("sparktest").enableHiveSupport().getOrCreate()
  //@transient
  private  val sc = sparkSession.sparkContext
  def main(args: Array[String]): Unit = {
    //val rdd = sparkSession.sparkContext.textFile("hdfs://10.27.1.138:9000/user/sopdm/ceshi.txt")
    val out_path = "hdfs://10.27.1.138:9000/user/sopdm/Resemble.txt"
    val nameDF = sparkSession.sql("SELECT gds_cd,gds_nm FROM BI_DM.DM_GDS_INF_TD WHERE l4_gds_group_cd='R1901001'").cache()
    val rdd = nameDF.rdd.map(item=>Tuple2(item(0).toString.trim,item(1).toString.trim))
    ///val cartesian = rdd cartesian rdd//笛卡尔积不建议使用
    //val allres = cartesian.map(item=>(item._1._1.toString,Map(item._2._1.toString->calResemble(item._1._2.toString().trim,item._2._2.toString.trim))))
    //val res = allres.reduceByKey((x,y)=>x++y).map(item=>(item._1,item._2.toArray.sortWith(_._2._3>_._2._3).take(10)))
    ///val allres = cartesian.map(item=>(item._1._1.toString,Map(item._2._1.toString->calResemble(item._1._2.toString().trim,item._2._2.toString.trim)))).take(10000)
    ///val res = sc.parallelize(allres).reduceByKey((x,y)=>x++y).map(item=>(item._1,item._2.toArray.sortWith(_._2._3>_._2._3).take(10)))
    //res.saveAsTextFile(out_path)
    ///res.foreach(item=>for(data<-item._2){
      ///println(item._1+":"+data._1+":"+data._2._1+":"+data._2._2+":"+data._2._3)
    ///})
    //===============================华丽分割线=========================================
    val datas = rdd.collect() //Tuple2[String,String],cd,name
    //val arrRes = ArrayBuffer[Map[String,Tuple3[String,Tuple2[String,String],Double]]]() //keycd1,keyname,cd2,name2,res
    val arrRes = List()
    for(i<- 0 until datas.length){//untildatas.length是不包含，to是包含datas.length
      for(j<- i+1 until datas.length){
        calResembleUpdata(datas(i),datas(j))+:arrRes
      }
    }

  }

  def getStrArr(str:String):Array[String]= {
    val regEx="[^0-9a-zA-Z\u4e00-\u9fa5]+".r
    val arr = ArrayBuffer[String]()
    val list = new JiebaSegmenter().sentenceProcess(str.trim)
    list.toArray().map(item=>regEx.replaceAllIn(item.toString," ")).filter(_!=" ").foreach(item=>if(item!=" ")arr+=item.toString.trim)
    arr.toArray
  }

  def calResemble(str1:String,str2:String):Tuple3[String,String,Double]= {
    val arr1 = getStrArr(str1)
    val arr2 = getStrArr(str2)
    Tuple3(str1,str2,calJaccard(arr1,arr2))
  }

  def calResembleUpdata(t1:Tuple2[String,String],t2:Tuple2[String,String]):Map[String,Tuple3[String,Tuple2[String,String],Double]]= {
    val arr1 = getStrArr(t1._2)
    val arr2 = getStrArr(t2._2)
    Map(t1._1->Tuple3(t1._2,Tuple2(t2._1,t2._2),calJaccard(arr1,arr2)))
  }

 /* def calResemble(str1:String,str2:String):Double= {
    val arr1 = getStrArr(str1)
    val arr2 = getStrArr(str2)
    calJaccard(arr1,arr2)
  }*/

  def calJaccard(arr1:Array[String],arr2:Array[String]): Double ={
    /*val rdd1 = sc.parallelize(arr1)
    val rdd2 = sc.parallelize(arr2)
    val rddunion = rdd1.union(rdd2).distinct()
    val rddinter = rdd1.intersection(rdd2)
    rddinter.count().toDouble/rddunion.count().toDouble*/
    /*val in = ArrayIntersection(arr1,arr2)
    val un = ArrayUnion(arr1,arr2)*/
    val in = arr2.intersect(arr1).distinct
    val un = arr1.union(arr2).distinct
    /*in.foreach(print)
    print("::")
    un.foreach(print)
    println()*/
    in.length.toDouble/un.length.toDouble
  }

}
