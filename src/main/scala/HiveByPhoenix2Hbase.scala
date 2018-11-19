import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}

object HiveByPhoenix2Hbase {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName(this.getClass.getName).setMaster("local[*]")

    val sc = new SparkContext(conf)

    val hiveContext = new HiveContext(sc)

    val sqlContext = new SQLContext(sc)

    hiveContext.sql("USE DW")

    val frame = hiveContext.sql("select * from dw_dbavgtime")


    val rdd = frame.rdd

    val historySchema = StructType({
      List(StructField("ROADIDONE", IntegerType),
        StructField("ROADIDTWO", IntegerType),
        StructField("DAYHOUR", IntegerType),
        StructField("TIME", DoubleType)
      )
    })

    val frame1 = sqlContext.createDataFrame(rdd, historySchema)

    frame1.write
    .format("org.apache.phoenix.spark")
    .mode("overwrite")
//    .option("table", "CROSSROADTEST")
    .option("table", "CROSSROADPRICE")
    .option("zkUrl", "192.168.145.79:2181")
    .save()
    frame.show()
  }
}