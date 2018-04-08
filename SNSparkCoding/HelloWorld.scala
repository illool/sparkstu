import scala.reflect.ClassTag
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.RowFactory

object HelloWorld {

  case class Student(val name:String) extends Ordered[Student]{
    override def compare(that:Student):Int={
      if(this.name==that.name)
        1
      else
        -1
    }
  }

  //将类型参数定义为T<:Ordered[T]
  class Pair1[T<:Ordered[T]](val first:T,val second:T){
    //比较的时候直接使用<符号进行对象间的比较
    def smaller()={
      if(first > second)
        first
      else
        second
    }
  }

  def foo[T](x: List[T])(implicit m: Manifest[T]) = {
  //def foo[T:Manifest] (x: List[T]){
    if (m <:< manifest[String])
      println("Hey, this list is full of strings")
    else
      println("Non-stringy list")
  }

  def arrayMake[T: Manifest](first: T, second: T) = {
    val r = new Array[T](2)
    r(0) = first
    r(1) = second
    r
  }
  def mkArray[T: ClassTag] (elems: T*) = Array[T](elems: _*)


  def main(args: Array[String]): Unit = {
    println("Hello World! gfj spark project......")
    val p=new Pair1(Student("摇摆少年梦"),Student("摇摆少年梦2"))
    println(p.smaller)
    foo(List("one", "two"))
    arrayMake(1, 2).foreach(println)
  }
}
