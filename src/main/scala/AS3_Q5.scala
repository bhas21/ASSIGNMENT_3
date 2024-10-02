import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window


object AS3_Q5 {
  def main(Args:Array[String]){
    //setting the logging level
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("Bhaskar")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._ // for toDF function

    val data=List(
      ("S001", "U001", 15, "2023-10-01", "Mobile", "Organic"),
      ("S002", "U002", 10, "2023-10-05", "Desktop", "Paid"),
      ("S003", "U003", 20, "2023-10-10", "Mobile", "Organic"),
      ("S004", "U004", 25, "2023-10-15", "Tablet", "Referral"),
      ("S005", "U001", 30 ,"2023-11-01", "Mobile", "Organic"))
      .toDF("session_id", "user_id", "page_views", "session_date", "device_type", "traffic_source")

    //data.show()

    val new_data=data.withColumn("session_month",month($"session_date"))
    new_data.show()

    data.filter(col("traffic_source")==="Organic" && col("device_type")==="Mobile").show()

    new_data.groupBy("device_type","session_month").agg(sum(col("page_views")).alias("Total_Page_Views"),
      avg(col("page_views").alias("Avg_Page_Views")),count(col("session_id").alias("Count_Of_Session"))).show()

    val window=Window.partitionBy("user_id").orderBy("session_id")

    val lead_lag_Data=data.select(
      col("session_id"),
      col("user_id"),
      col("page_views"),
      col("session_date"),
      col("device_type"),
      col("traffic_source"),
      lag(col("page_views"),1).over(window).alias("Previous_Page_Views"),
      lead(col("page_views"),1).over(window).alias("Next_Page_Views"))

    lead_lag_Data.show()
    spark.stop()

  }

}