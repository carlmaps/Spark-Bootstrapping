import org.apache.spark.sql.functions.{avg, col, mean, sum, var_pop, var_samp}
import org.apache.spark.sql.{SQLContext, SparkSession, functions}
import scala.::


object InsuranceCost extends App{

  override def main(args: Array[String]): Unit ={

    val path = "file:///results"
    val spark = SparkSession.builder()
      .master("local")
      .appName("InsuranceCost")
      .config("spark.sql.warehouse.dir", path)
      .getOrCreate();

    val sqlContext:SQLContext = spark.sqlContext

    //read csv with options
    val insuranceDF = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("sep", ",").csv("insurance.csv").cache()


    insuranceDF.show()
    insuranceDF.printSchema()

    //Get sample data from the main data frame
    val sampledData = insuranceDF.sample(false, 0.5)

    val counter = 0
    var bsc_aveAgeFemale = List[Double]()
    var bsc_aveAgeMale = List[Double]()

    var bsc_aveBMIFemale = List[Double]()
    var bsc_aveBMIMale = List[Double]()

    var bsc_aveChargesFemale = List[Double]()
    var bsc_aveChargesMale = List[Double]()

    var bsc_varAgeFemale = List[Double]()
    var bsc_varAgeMale = List[Double]()

    var bsc_varBMIFemale = List[Double]()
    var bsc_varBMIMale = List[Double]()

    var bsc_varChargesFemale = List[Double]()
    var bsc_varChargesMale = List[Double]()

    //Resampling 10 times
    for(counter <- 1 to 10){

      //getting average by sex
      val reSampledData = insuranceDF.sample(true,1)
      val mvBySex = reSampledData.groupBy("sex").agg(avg("age"), avg("bmi"), avg("charges"),
        functions.variance("age"), functions.variance("bmi"), functions.variance("charges")).cache()

      val aveAgeFemaleByGender = mvBySex.collect()(0)(1)
      val aveAgeMaleByGender = mvBySex.collect()(1)(1)

      val aveBMIFemaleByGender = mvBySex.collect()(0)(2)
      val aveBMIMaleByGender = mvBySex.collect()(1)(2)

      val aveChargesFemaleByGender = mvBySex.collect()(0)(3)
      val aveChargesMaleByGender = mvBySex.collect()(1)(3)

      val varAgeFemaleByGender = mvBySex.collect()(0)(4)
      val varAgeMaleByGender = mvBySex.collect()(1)(4)

      val varBMIFemaleByGender = mvBySex.collect()(0)(5)
      val varBMIMaleByGender = mvBySex.collect()(1)(5)

      val varChargesFemaleByGender = mvBySex.collect()(0)(6)
      val varChargesMaleByGender = mvBySex.collect()(1)(6)

      bsc_aveAgeFemale = bsc_aveAgeFemale :+ aveAgeFemaleByGender.asInstanceOf[Double]
      bsc_aveAgeMale = bsc_aveAgeMale :+ aveAgeMaleByGender.asInstanceOf[Double]

      bsc_aveBMIFemale = bsc_aveBMIFemale :+ aveBMIFemaleByGender.asInstanceOf[Double]
      bsc_aveBMIMale = bsc_aveBMIMale :+ aveBMIMaleByGender.asInstanceOf[Double]

      bsc_aveChargesFemale = bsc_aveChargesFemale :+ aveChargesFemaleByGender.asInstanceOf[Double]
      bsc_aveChargesMale = bsc_aveChargesMale :+ aveChargesMaleByGender.asInstanceOf[Double]

      bsc_varAgeFemale = bsc_varAgeFemale :+ varAgeFemaleByGender.asInstanceOf[Double]
      bsc_varAgeMale = bsc_varAgeMale :+ varAgeMaleByGender.asInstanceOf[Double]

      bsc_varBMIFemale = bsc_varBMIFemale :+ varBMIFemaleByGender.asInstanceOf[Double]
      bsc_varBMIMale = bsc_varBMIMale :+ varBMIMaleByGender.asInstanceOf[Double]

      bsc_varChargesFemale = bsc_varChargesFemale :+ varChargesFemaleByGender.asInstanceOf[Double]
      bsc_varChargesMale = bsc_varChargesMale :+ varChargesMaleByGender.asInstanceOf[Double]

    }

    var bsAveAgeFemale = bsc_aveAgeFemale.sum / bsc_aveAgeFemale.length
    var bsAveAgeMale = bsc_aveAgeMale.sum / bsc_aveAgeMale.length

    var bsAveBMIFemale = bsc_aveBMIFemale.sum / bsc_aveBMIMale.length
    var bsAveBMIMale = bsc_aveBMIMale.sum / bsc_aveBMIMale.length

    var bsAveChargesFemale = bsc_aveChargesFemale.sum / bsc_aveChargesFemale.length
    var bsAveChargesMale = bsc_aveChargesMale.sum / bsc_aveChargesMale.length

    var bsVarAgeFemale = bsc_varAgeFemale.sum / bsc_varAgeFemale.length
    var bsVarAgeMale = bsc_varAgeMale.sum / bsc_varAgeMale.length

    var bsVarBMIFemale = bsc_varBMIFemale.sum / bsc_varBMIFemale.length
    var bsVarBMIMale = bsc_varBMIMale.sum / bsc_varBMIMale.length

    var bsVarChargesFemale = bsc_varChargesFemale.sum / bsc_varChargesFemale.length
    var bsVarChargesMale = bsc_varChargesMale.sum / bsc_varChargesMale.length


    val initialDF = sampledData.groupBy("sex").agg(avg("age"), avg("bmi"), avg("charges"),
      functions.variance("age"), functions.variance("bmi"), functions.variance("charges")).cache()

    initialDF.show(false)
    initialDF.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("initialDF")

    import spark.implicits._

    val arraylist: Array[(String, Double, Double, Double, Double, Double, Double)] = Array(("female", bsAveAgeFemale, bsAveBMIFemale, bsAveChargesFemale, bsVarAgeFemale, bsVarBMIFemale, bsVarChargesFemale),
            ("male", bsAveAgeMale, bsAveBMIMale, bsAveChargesMale, bsVarAgeMale, bsVarBMIMale, bsVarChargesMale))

    val sc = spark.sparkContext
    val finalDF = sc.parallelize(arraylist).toDF("sex", "mean(age)", "mean(bmi)", "mean(charges)", "variance(age)", "variance(bmi)", "variance(charges)").cache()

    finalDF.show(false)
    finalDF.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("finalDF")

  }

}
