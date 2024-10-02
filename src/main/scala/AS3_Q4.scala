import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window


object AS3_Q4 {
  def main(Args:Array[String]){
    //setting the logging level
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("Bhaskar")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._ // for toDF function

    val data=List(
      ( "L001", "C001", 1000, "2023-11-01", "2023-12-01", "Personal Loan", "7.5%"),
      ("L002", "C002", 2000, "2023-11-15", "2023-11-20", "Home Loan", "6.5%"),
      ("L003", "C003", 1500, "2023-12-01", "2023-12-25", "Personal Loan", "8.0%"),
      ("L004", "C004", 2500 ,"2023-12-10", "2024-01-15", "Car Loan" ,"9.0%"),
      ("L006", "C004", 2500 ,"2023-12-18", "2024-01-18", "Home Loan" ,"9.0%"),
      ("L005", "C005", 1200, "2023-12-15", "2024-01-20", "Personal Loan", "7.8%")
    ).toDF("loan_id", "customer_id", "repayment_amount", "due_date", "payment_date",
      "loan_type", "interest_rate")

    //data.show()

    val new_data=data.withColumn("Repayment_Delay",datediff($"payment_date",$"due_date"))
    new_data.show()

    new_data.filter(col("loan_type").startsWith("Personal") && col("Repayment_Delay")>30).show()


    new_data.groupBy("loan_type", "interest_rate").agg(sum(col("repayment_amount")).alias("Total_Amount"),
      max(col("Repayment_Delay")).alias("Max_Repayment_Delay")
      ,avg(col("interest_rate")).alias("Avg_Interest_Rate")).show()


    val window=Window.partitionBy("customer_id").orderBy("payment_date")

    val lead_data=data.select(col("loan_id"),col("customer_id"),col("repayment_amount"),col("due_date"),
      col("payment_date"),col("loan_type"),col("interest_rate"),lead(col("payment_date"),1).over(window)
        .alias("Next_Repayment_Date"))

    lead_data.show()
    spark.stop()

  }

}