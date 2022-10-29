import org.apache.spark.sql.SparkSession

object DF {
  def main(args:Array[String]) :Unit ={
    val spark = SparkSession.builder().master("local").appName("df").getOrCreate()
    val data = Seq(("ayyappa","2000"),("trijan","2000"),("gopi","3000"))
    import spark.implicits._
    val df=data.toDF("name","salary")
    df.show()
  }

}
