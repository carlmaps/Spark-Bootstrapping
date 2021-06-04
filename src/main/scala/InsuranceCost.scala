import org.apache.spark.sql.{SQLContext, SparkSession}

object InsuranceCost extends App{

  override def main(args: Array[String]): Unit ={

    val spark = SparkSession.builder()
      .master("local")
      .appName("InsuranceCost")
      .getOrCreate();

    val sqlContext:SQLContext = spark.sqlContext

    //read csv with options
    val df = sqlContext.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
      .csv("insurance.csv")
    df.show()
    df.printSchema()

  }

}
