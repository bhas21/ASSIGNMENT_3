import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window


object AS3_Q6 {
  def main(Args:Array[String]){
    //setting the logging level
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("Bhaskar")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._ // for toDF function

    val data=List(
      ("R001", "O001", "Electro Gadgets", "2023-12-01", "Damaged", 100),
      ("R002" ,"O002", "Home Appliances" ,"2023-12-05" ,"Defective", 50),
      ("R003", "O003", "Electro Toys", "2023-12-10", "Changed Mind", 75),
      ("R004", "O004", "Electro Gadgets", "2023-12-15", "Damaged", 100),
      ("R005", "O005", "Kitchen Set" ,"2023-12-20" ,"Wrong Product" ,120) )
      .toDF("return_id", "order_id", "product_name", "return_date", "return_reason", "refund_amount")


    val return_year_df=data.withColumn("return_year", year($"return_date"))

    return_year_df.show()

    val filter_data=data.filter(col("product_name").startsWith("Electro") && col("refund_amount").isNotNull)

    filter_data.show()

    return_year_df.groupBy("return_year","return_reason").
      agg(sum(col("refund_amount")).alias("Total_Refund_Anount")
        ,count(col("return_id")).alias("Total_Returns")
        ,avg(col("refund_amount")).alias("Avg_Refund_Amount")).show()



    val window=Window.partitionBy("product_name").orderBy("return_date")
    val lag_data=data.select(col("*"),
      lag(col("refund_amount"),1).over(window).alias("Previous_Return_Amount"))

    lag_data.show()

    spark.stop()
  }

}