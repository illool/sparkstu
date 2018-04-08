import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{Vector, Vectors}
import scala.collection.mutable.ListBuffer
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import scala.collection.mutable.ArrayBuffer;

object l4sumKNN {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("l4sumKNN").enableHiveSupport().getOrCreate()
    val l4sumDF = sparkSession.sql("select l4_gds_group_cd,l4_gds_group_desc,sum(pay_amt) as l4sum from" +
      "(" +
      "select * from BI_DPA.TDPA_OR_ORDER_DETAIL_D where statis_date >='20170901' and statis_date <='20180301' and pay_date = statis_date"
      + ")" +
      "group by l4_gds_group_cd,l4_gds_group_desc order by l4sum desc"
    )
    //.cache()
    val sumdata = l4sumDF.select("l4sum")
    import sparkSession.implicits._
    //case class model_instance(features: Vector)
    val arr = Array[String]("l4sum")
    val schema = StructType(arr.map(fieldName => StructField(fieldName, VectorType, false)))
    println(schema.treeString)
    val rowRDD = sumdata.rdd.map(item => Row(Vectors.dense(item.getDouble(0))))
    //println(rowRDD.getClass)
    val resDataFrame = sparkSession.createDataFrame(rowRDD, schema)
    //println(resDataFrame.getClass)
    //resDataFrame.foreach(item => print(item))
    //print(resDataFrame.schema.treeString)
    val kmeans = new KMeans().setK(2).setFeaturesCol("l4sum").setPredictionCol("prediction").setMaxIter(100).setTol(0.5)
    //val para = new ParamMap()
    val model = kmeans.fit(resDataFrame)
    val predictions = model.transform(resDataFrame)
    predictions.show()
    predictions.collect().take(10).foreach(row => {
      print(row(0) + " is predicted as cluster " + row(1))
    })

    model.clusterCenters.foreach(center => {
      print("Clustering Center:" + center)
    })
    val err = model.computeCost(resDataFrame)
    println(err)
    var str = ""
    val reslist = new ListBuffer[String]()
    val test = for (i <- 2 to 30) yield {
      val kmeans = new KMeans().setK(i).setFeaturesCol("l4sum").setPredictionCol("prediction").setMaxIter(100).setTol(0.5)
      val model = kmeans.fit(resDataFrame)
      //val predictions = model.transform(resDataFrame)
      //predictions.collect().take(10).foreach(row => {
      //print( row(0) + " is predicted as cluster " + row(1))})
      //model.clusterCenters.foreach(center => {print("Clustering Center:"+center)})
      val err = model.computeCost(resDataFrame)
      //var resmap = Map(i -> err)
      str = i + ":" + err
      reslist.append(str)
      str
    }
    reslist.foreach(item => println(item))
    test.foreach(println)

    /*
        import sparkSession.implicits._
        val dataList: List[(Double, String, Double, Double, String, Double, Double, Double, Double)] = List(
          (0, "male", 37, 10, "no", 3, 18, 7, 4),
          (0, "female", 27, 4, "no", 4, 14, 6, 4),
          (0, "female", 32, 15, "yes", 1, 12, 1, 4),
          (0, "male", 57, 15, "yes", 5, 18, 6, 5),
          (0, "male", 22, 0.75, "no", 2, 17, 6, 3),
          (0, "female", 32, 1.5, "no", 2, 17, 5, 5))

        val data = dataList.toDF("affairs", "gender", "age", "yearsmarried", "children", "religiousness", "education", "occupation", "rating")
        data.show(10)
    */
  }
}
