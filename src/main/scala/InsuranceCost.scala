import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.{SQLContext, SparkSession}

object InsuranceCost extends App{

  override def main(args: Array[String]): Unit ={

    val spark = SparkSession.builder()
      .master("local")
      .appName("InsuranceCost")
      .getOrCreate();

    val sqlContext:SQLContext = spark.sqlContext

    //read csv with options
    val insuranceDF = spark.read.format("csv")
                  .option("inferSchema","true")
                  .option("header", "true")
                  .option("sep", ",").csv("insurance.csv").cache()


//    val df = sqlContext.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
//      .csv("insurance.csv")
    insuranceDF.show()
    insuranceDF.printSchema()

    val counter = 0
    for(counter <- 1 to 10){
      val sampledData = insuranceDF.sample(false, 0.5, 1)
      val aveAge = sampledData.groupBy("sex").agg(avg("age")).show()
      val aveBMI = sampledData.groupBy("sex").agg(avg("bmi")).show()
      val aveCharges = sampledData.groupBy("sex").agg(avg("charges")).show()
    }

  }

}
