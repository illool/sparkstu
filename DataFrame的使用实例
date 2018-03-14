import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

object Prmtr {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("JdbcDbAccess")
    val sc = new SparkContext(conf)
    //SQLContext被遗弃的方法，SparkSession
    //val sqlContext = new SQLContext(sc)
    val sparkSession = SparkSession.builder().appName("sparktest").enableHiveSupport().getOrCreate()
    //val pDF = sparkSession.sql("SELECT gds_id,catgroup_id,prmtr_nm,prmtr_value_nm,prmtr_cd,prmtr_value_cd FROM bi_td.tdm_ca_prmtr_td limit 10000")
    val pDF = sparkSession.sql("SELECT gds_id,catgroup_id,prmtr_nm,prmtr_value_nm FROM bi_td.tdm_ca_prmtr_td where catgroup_id = \"R1901001\" ").cache()
    val infoDF = sparkSession.sql("SELECT gds_cd,gds_nm FROM bi_dm.dm_gds_inf_td where l4_gds_group_cd = \"R1901001\" ").cache()
    //val unionDF = pDF.join(infoDF, pDF("gds_id") === infoDF("gds_cd"))
    //test.show(10)
    //unionDF.printSchema()
    //infoDF.show(10)
    //pDF.show()
    //val pDFs =  pDF.rdd.map(item=>(item(0).toString,Map( "catgroup_id"->item(1).toString,item(2).toString -> item(3).toString,item(4).toString -> item(5).toString))).reduceByKey((x,y)=>x++y)
    val pDFs =  pDF.rdd.map(item=>(item(0).toString,Map( "catgroup_id"->item(1).toString,item(2).toString -> item(3).toString))).reduceByKey((x,y)=>x++y)
    //val pmapDFs = pDFs.map(item=>Map(item._1->Map(item._2.get("catgroup_id").toString->item._2)))
    /*val pmapDFs = pDFs.map(item=>(item._2.get("catgroup_id").get->item._2.filter(_._1.toString.!=("catgroup_id"))))
    val cpDFs = pmapDFs.reduceByKey((x,y)=>x++y).map(item=>(item._1->item._2.keySet))*/
    val infoDFs =  infoDF.rdd.map(item=>(item(0).toString,Map( "商品名称" -> item(1).toString))).reduceByKey((x,y)=>x++y)
    //val unionDF = pDFs.join(infoDFs).groupByKey.reduceByKey((x,y)=>x++y)
    //val unionDF = pDFs.leftOuterJoin(infoDFs).mapValues( item => item._1 ++ item._2 ).reduceByKey((x,y)=>x++y)
    val unionDF = pDFs.join(infoDFs).mapValues( item => item._1 ++ item._2 )
    val pmapDFs = unionDF.map(item=>(item._2.get("catgroup_id").get->item._2.filter(_._1.toString.!=("catgroup_id")))).sortByKey()
    val cpDFs = pmapDFs.reduceByKey((x,y)=>x++y).map(item=>(item._1->item._2.keySet))//Tuple2(String,Set)

    /*val num = cpDFs.count()
    val rddList = cpDFs.take(num.toInt)
    for ( i <- rddList) {
      println("Your RDD element here: " + i.getClass)
    }*/
    val col = cpDFs.lookup("R1901001")
    //val len = col.take(0).length
    //val numArr = new Array[String](len)
    val arr = ArrayBuffer[String]()
    arr+="gds_id"
    for(item<-col){
      for(str<-item){
        //println("Your RDD element here: " + str + arr.length +str.getClass)
        arr += str.trim
      }
    }
    //val schemaString = "name age"
    import org.apache.spark.sql.types.{StructType,StructField,StringType};
    val schema = StructType(arr.toArray.map(fieldName=>StructField(fieldName,StringType,true)))
    val rowRDD = unionDF.map(item=>getDataRow(item,arr.toArray))
    val resDataFrame = sparkSession.createDataFrame(rowRDD,schema)
    //resDataFrame.show()
    //resDataFrame.printSchema()
    val len  = arr.length
    //println(len)
    val arrRes = ArrayBuffer[Tuple3 [String,String,String]]()
    for(i<- 0 until len){
      val temp = resDataFrame.select(arr(i))
      val d = temp.count().toFloat
      val n = temp.filter(_.get(0)!="").count().toFloat
      val res = n/d
      arrRes += new Tuple3(arr(i), res.toString,n.toString)
    }
    arrRes.toArray.foreach(item=>println(item._1+":"+item._2+":"+item._3))
    /*val bjms = resDataFrame.select(arr(45))
    bjms.show()
    println(bjms.count()+arr(45)+"================")
    bjms.foreach(item=>if(null!=item(0)&&item(0)!="") println(arr(2)+":"+item(0)))
    println(arr(45)+bjms.filter(_.get(1)!="").count())
    println(bjms.count())*/
    //resDataFrame.show()
    //resDataFrame.take(10).foreach(println)
    //println("+++++++++++++++++++++++++++++++++"+rowRDD.toString)
    /*unionDF.map(item=>Row(
      item._1.toString,
    ))*/
    //val schema = StructType(schemaString.split(" ").map(fieldName=>StructField(fieldName,StringType,true)))
    /*val rowRDD = people.map(_.split(",")).map(p=>Row(p(0),p(1).trim))
    val peopleDataFrame = sparkSession.createDataFrame(rowRDD,schema)*/
    //schema.map(item=>println(item.name+item.dataType+item.metadata))
    //按key排序：SortedMap
    //按添加顺序：LinkedHashMap
    /*val map1 = Map(("sa", 1), ("s", 2))
    for ((k, v) <- map1) {
      print(k)
      println(v)
    }*/
    /*val schemaString = "name age"
    import org.apache.spark.sql.types.{StructType,StructField,StringType};
    val schema = StructType(schemaString.split(" ").map(fieldName=>StructField(fieldName,StringType,true)))*/
    //people是数据，得到两个值
    //val rowRDD = people.map(_.split(",")).map(p=>Row(p(0),p(1).trim))
    //val peopleDataFrame = sqlContext.createDataFrame(rowRDD,schema)
    //schema.foreach(println)
    //pDFs.take(10).foreach(println)
    //pmapDFs.take(10).foreach(println)
    ///cpDFs.take(10).foreach(_._2.foreach(println))
    ////infoDFs.take(100).foreach(println)
    //unionDF.take(1).foreach(getDataRow(_,arr.toArray))

    /*val rdd1 = sc.makeRDD(1 to 2,1)
    val rdd2 = sc.makeRDD(2 to 100,1)
    rdd1.union(rdd2).collect().foreach(println)*/
    //greet()
    //println("The calss is :"+cpDFs.getClass)
  }

  def greet() = println("Hello, world!")
  def getDataRow(data:Tuple2[String,Map[String,String]],keys:Array[String]):Row = {
    val arr = ArrayBuffer[String]()
    arr+=data._1.toString
    for(key<-keys){
      if(key!="gds_id") {
        val temp = data._2.get(key)
        if (temp.isEmpty) {
          arr += ""
          //println(key + ":" + "-")
        }
        else {
          arr += temp.get
          //println(key + ":" + temp.get)
        }
      }
    }
    //println(arr.toString())
    /*var i = 0
    for(key<-keys){
      println(key+":"+arr(i))
      i+=1
    }*/
    Row.fromSeq(arr.toArray.toSeq)
  }
}
