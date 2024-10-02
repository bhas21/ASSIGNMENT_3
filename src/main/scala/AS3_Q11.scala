import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window


object AS3_Q11 {
  def main(Args:Array[String]){
    //setting the logging level
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("Bhaskar")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._ // for toDF function
    val customer_purchases = List(
      ("P001", "C001", "Running Shoes", "2023-10-10", 100, "Credit Card"),
      ("P002", "C002", "Basketball Shoes", "2023-11-01", 150, "Debit Card"),
      ("P003", "C003", "Soccer Shoes", "2023-12-01", 120, "Credit Card"),
      ("P004", "C001", "Dress Shoes", "2024-01-15", 200, "Credit Card"),
      ("P005", "C004", "Running Shoes", "2024-02-10", 110, "Cash"),
      ("P006", "C001", "Casual Shoes", "2024-02-15", 90, "Credit Card"),
      ("P007", "C002", "Sneakers", "2024-03-01", 130, "Credit Card"),
      ("P008", "C003", "Formal Shoes", "2024-03-05", 160, "Credit Card"),
      ("P009", "C001", "Sports Shoes", "2024-03-10", 115, "Credit Card"),
      ("P010", "C002", "Running Shoes", "2024-04-01", 140, "Debit Card"))
      .toDF("purchase_id", "customer_id", "product_name", "purchase_date", "purchase_amount", "payment_method")


    val new_data=customer_purchases.withColumn("purchase_month",month($"purchase_date"))

    new_data.show()

    val filtered_data=customer_purchases.filter(col("product_name").endsWith("Shoes") &&
      col("payment_method").startsWith("Credit"))

    filtered_data.show()

    val groupped_data=new_data.groupBy("customer_id", "purchase_month").
      agg(sum("purchase_amount").alias("Total_Puchase_Amount"),
        min(col("purchase_amount").alias("Min_Purchase_Amount")),
        count(col("purchase_amount").alias("Count_Of_Purchases")))

    groupped_data.show()


    val window=Window.partitionBy("customer_id").orderBy("purchase_date")

    val lead_data=customer_purchases.select(col("*"),
      lead(col("purchase_amount"),1).over(window).alias("Next_Purchase_Amount"),
      (lead(col("purchase_amount"),1).over(window)-col("purchase_amount")).
        alias("Differences"))

    lead_data.show()

    spark.stop()
  }

}