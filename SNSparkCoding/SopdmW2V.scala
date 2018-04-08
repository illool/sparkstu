import com.huaban.analysis.jieba.JiebaSegmenter
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object SopdmW2V {
  def main(args: Array[String]): Unit = {//SparkContext.setSystemProperty('spark.driver.maxResultSize', '10g')
   val out_path = "hdfs://10.27.1.138:9000/user/sopdm/word2vec.gfj"
    val sparkSession = SparkSession.builder().appName("sparktest").enableHiveSupport().getOrCreate()
    val nameDF = sparkSession.sql("SELECT gds_nm FROM BI_DM.DM_GDS_INF_TD").cache()
    val chars = nameDF.rdd.map(item=>getStrArr(item(0).toString.trim))
    val documentDF = sparkSession.createDataFrame(chars.map(Tuple1.apply)).toDF("namelist")
    //chars.saveAsTextFile(out_path)

    //documentDF.take(10).foreach(row=>println(row))
    val word2Vec = new Word2Vec()
      .setInputCol("namelist")
      .setOutputCol("result")
      .setVectorSize(50)
      .setMinCount(1)
    val model = word2Vec.fit(documentDF)
    val result = model.transform(documentDF)

    val vecs = model.getVectors
    //vecs.rdd.foreach(println)
    vecs.rdd.saveAsTextFile(out_path)
  }

  def getStrArr(str:String):Seq[String]= {
    val regEx="[^0-9a-zA-Z\u4e00-\u9fa5]+".r
    val arr = ArrayBuffer[String]()
    val list = new JiebaSegmenter().sentenceProcess(str.trim)
    list.toArray().map(item=>regEx.replaceAllIn(item.toString," ")).filter(_!=" ").foreach(item=>if(item!=" ")arr+=item.toString.trim)
    arr.toArray.toSeq
  }
}
