import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window


object AS3_Q12 {
  def main(Args:Array[String]){
    //setting the logging level
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("Bhaskar")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._ // for toDF function
    val product_stock = List(
      ("P001", "Mobile Phone", "Electronics", "2023-10-01", 80, "Supplier A"),
      ("P002", "Washing Machine", "Home Appliances", "2023-11-05", 120, "Supplier B"),
      ("P003", "Laptop", "Electronics", "2023-12-10", 60, "Supplier A"),
      ("P004", "TV", "Consumer Electronics", "2023-12-15", 95, "Supplier C"),
      ("P005", "Mobile Phone", "Electronics", "2024-01-01", 40, "Supplier A"),
      ("P006", "Tablet", "Electronics", "2024-01-10", 70, "Supplier B"),
      ("P007", "Camera", "Electronics", "2024-01-15", 30, "Supplier C"),
      ("P008", "Headphones", "Electronics", "2024-02-01", 50, "Supplier A"),
      ("P009", "Bluetooth Speaker", "Electronics", "2024-02-05", 20, "Supplier B"),
      ("P010", "Smart Watch", "Electronics", "2024-02-10", 90, "Supplier C")
    ).toDF("product_id", "product_name", "category", "stock_date", "stock_level", "supplier")



    val new_data=product_stock.withColumn("stock_month",month($"stock_date"))

    new_data.show()

    val filtered_data=product_stock.filter(col("category").startsWith("Electro") && col("stock_level")<100)

    filtered_data.show()

    val groupped_data=new_data.groupBy("stock_month","supplier")
      .agg(avg(col("stock_level")).alias("Avg_Stock_Level"),
        max(col("stock_level")).alias("Max_Stock_Level"),
        count(when(col("stock_level")<50,1).alias("Less_Stock")))
    groupped_data.show()

    val window=Window.partitionBy("product_name").orderBy("stock_date")

    val lag_data=product_stock.select(col("*"),
      lag(col("stock_level"),1).over(window).alias("Previous_Stock"))

    lag_data.show()

    spark.stop()
  }

}