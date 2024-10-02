import org.apache.spark
import org.apache.spark.sql.{SaveMode, SparkSession, functions => F}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.Dataset


object AS3_Q8 {
  def main(Args:Array[String]): Unit = {
    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("Bhaskar")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._ // for toDF function
    val data = List(
      ("P001", "Mobile Phone", "2023-10-01", 500, "Electronics"),
      ("P002", "Washing Machine", "2023-10-05", 700, "Home Appliances"),
      ("P003", "Laptop", "2023-10-10", 1200, "Electronics"),
      (  "P004", "TV", "2023-10-15", 1000, "Consumer Electronics"),
      ("P005", "Mobile Phone", "2023-11-01", 550, "Electronics")
    ).toDF("product_id", "product_name", "price_date", "price", "category")

    val new_data=data.withColumn("price_month",month($"price_date"))

    new_data.show()

    val window=Window.partitionBy("product_name").orderBy("price_date")

    val next_month_data=new_data.select(col("*"),lead(col("price"),1).over(window).alias("Next_Month_Price"))

    next_month_data.show()

    val filtered_data=next_month_data.filter(col("category").endsWith("Electronics") && col("Next_Month_Price")>
      col("price"))

    filtered_data.show()
//val groupped_data=groupBy.


spark.stop()
  }
}
