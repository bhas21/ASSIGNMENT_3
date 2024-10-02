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

object AS3_Q1 {
  def main(Args:Array[String]): Unit = {
    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("Bhaskar")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._ // for toDF function
val data=List(
  ("E001", "Sales", 85, "2024-02-10", "Sales Manager"),
    ("E002" ,"HR", 78, "2024-03-15" ,"HR Assistant"),
    ("E003", "IT", 92, "2024-01-22", "IT Manager"),
    ("E004", "Sales", 88, "2024-02-18", "Sales Rep"),
    ("E005" ,"HR", 95 ,"2024-03-20" ,"HR Manager"),
  ("E006", "IT", 96, "2024-01-22", "IT Manager")
).toDF("employee_id", "department", "performance_score", "review_date", "position")

   val new_data= data.withColumn("Review_Month",month($"review_date"))
    new_data.show()

    data.filter(col("performance_score")>80).filter(col("position").endsWith("Manager")).show()

    val agg_data=new_data.groupBy("Review_Month","department").
      agg(avg("performance_score").alias("Avg_Performance_Score"),
        count(when(col("performance_score")>90,1)).alias("EMP_COUNT"))
    agg_data.show()

    /*val avg_data=new_data.groupBy("Review_Month","department").
      agg(avg("performance_score").alias("Avg_Performance_Score"))
    avg_data.show()

   val count_data= new_data.groupBy("Review_Month","department").
      agg(count(when(col("performance_score")>90,1)).alias("EMP_COUNT"))
    count_data.show()*/

val emp_lag=new_data.withColumn("Previous_Performance",lag("performance_score",1).
over(Window.partitionBy("employee_id").orderBy("review_date")))

    emp_lag.show()

    val emp_improvement=emp_lag.withColumn("Performance_Improvement",col("performance_score")-
      col("Previous_Performance"))

    emp_improvement.show()





    //data.show()

    spark.stop()
  }


}
