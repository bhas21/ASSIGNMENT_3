import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window


object AS3_Q2 {
  def main(Args:Array[String]){
    //setting the logging level
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("Bhaskar")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._ // for toDF function

    val data=List(
      ( "C001", "Premium Gold", "Yes", "2023-12-01", 1200, "USA"),
      ("C002", "Basic", "No" ,null ,400, "Canada"),
      ("C003", "Premium Silver", "Yes", "2023-11-15", 800, "UK"),
      ("C004", "Premium Gold", "Yes", "2024-01-10", 1500, "USA"),
      ("C005" ,"Basic", "No",null, 300, "India")).toDF("customer_id",
      "subscription_type", "churn_status", "churn_date", "revenue", "country")

    //data.show()

    val year_data= data.withColumn("Churn_Year",year($"churn_date"))
    year_data.show()

    val premium_data=data.filter(col("subscription_type").startsWith("Premium")).
      filter(col("churn_status").isNotNull)

    premium_data.show()

    val groupped_data=year_data.filter(col("churn_date").isNotNull)
      .groupBy("country","Churn_Year")
      .agg(avg("revenue").alias("Avg_Revenue"),sum("revenue").alias("Total_Revenue_Lost"),
        count("customer_id").alias("Count_Of_Churned_Customers"))


    groupped_data.show()

    val window=Window.partitionBy("country").orderBy("churn_date")

    val df=data.select(col("*"), lead(col("revenue"),1).over(window).alias("Next_Year")).show()


    spark.stop()
  }
}