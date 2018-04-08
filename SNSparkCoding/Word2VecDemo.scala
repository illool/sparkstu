import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.Word2Vec

object Word2VecDemo {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("sparktest").getOrCreate()
    val documentDF = sparkSession.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")
  /*
    Spark 的 Word2Vec 实现提供以下主要可调参数：

    inputCol , 源数据 DataFrame 中存储文本词数组列的名称。
    outputCol, 经过处理的数值型特征向量存储列名称。
    vectorSize, 目标数值向量的维度大小，默认是 100。
    windowSize, 上下文窗口大小，默认是 5。
    numPartitions, 训练数据的分区数，默认是 1。
    maxIter，算法求最大迭代次数，小于或等于分区数。默认是 1.
    minCount, 只有当某个词出现的次数大于或者等于 minCount 时，才会被包含到词汇表里，否则会被忽略掉。
    stepSize，优化算法的每一次迭代的学习速率。默认值是 0.025.
  */
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(10)
      .setMinCount(1)
    val model = word2Vec.fit(documentDF)
    val result = model.transform(documentDF)
    result.rdd.foreach(println)

    val vecs = model.getVectors
    vecs.rdd.foreach(println)
    //val out_path = "./data/word2vec"
    //vecs.rdd.saveAsTextFile(out_path)
  }
}
