import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window


object AS3_Q3 {
  def main(Args:Array[String]){
    //setting the logging level
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("Bhaskar")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._ // for toDF function

    val data=List(
      ("S001", 15000, 12000, "2023-12-10", "North", "Electronics Accessories"),
      ("S001", 12000, 12000, "2023-12-11", "North", "Electronics Accessories"),
      ("S002" ,8000 ,9000 ,"2023-12-11" ,"South", "Home Appliances"),
      ("S003", 20000, 18000, "2023-12-12", "East", "Electronics Gadgets"),
      ("S004" ,10000 ,15000, "2023-12-13" ,"West", "Electronics Accessories"),
      ("S005", 18000, 15000, "2023-12-14", "North", "Furniture Accessories")
    ).toDF("salesperson_id", "sales_amount", "target_amount", "sale_date", "region", "product_category")

    val new_data=data.withColumn("target_achieved",when(col("sales_amount")>=col("target_amount"),"Yes").otherwise("No"))

    new_data.show(false)

    val filtered_data=data.filter(col("product_category").startsWith("Electronics").endsWith("Accessories"))

    filtered_data.show(false)

    new_data.groupBy("region","product_category").agg(sum(col("sales_amount")).alias("Total_Sales")
        ,min(col("sales_amount")).alias("Min_Sales")
        // ,count(col("salesperson_id").when(col("target_achieved")==="Yes",1).alias("Count_Of_Salesperson"))
      )
      .show(false)

    new_data.filter(col("target_achieved")==="Yes")
      .groupBy("region","product_category").agg(count(col("salesperson_id")).alias("Count_Of_Salesperson"))
      .show()

    val window=Window.partitionBy("salesperson_id").orderBy("sale_date")

    val lag_data=data.select(col("salesperson_id"),col("sales_amount"),col("target_amount"),col("sale_date"),
      col("region"),col("product_category"),lag(col("sales_amount"),1).over(window).alias("lag_sales_amount"))

    lag_data.show()




    spark.stop()

  }

}