import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window


object AS3_Q9 {
  def main(Args:Array[String]){
    //setting the logging level
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("Bhaskar")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._ // for toDF function

    val data=List(
      ("A001", "E001", "2023-11-01", "Present", "Sales", "Day"),
      ("A002", "E002", "2023-11-02", "Absent", "HR", "Night"),
      ("A003", "E003", "2023-11-03", "Present", "IT", "Night"),
      ("A004", "E001", "2023-11-04", "Absent", "Sales", "Night"),
      ("A005", "E002", "2023-11-05", "Present", "HR", "Day")
    ).toDF("attendance_id", "employee_id", "attendance_date", "status", "department", "shift")

    val new_data=data.withColumn("attendance_month",month($"attendance_date"))

    new_data.show()

    val filtered_data=data.filter(col("status")==="Absent" && col("shift")==="Night")


    filtered_data.show()

    val groupped_data=new_data.groupBy("department","attendance_month").
      agg(count(when(col("status")==="Absent",1)).alias("Total_Absent_Count"),
        avg(when(col("status")==="Absent",1)).alias("Avg_Absent_Count"))

    groupped_data.show()



    val window=Window.partitionBy("employee_id").orderBy("attendance_date")

    val lead_data=data.select(col("*"),
      lead(col("status"),1).over(window).alias("Next_Status"))

    lead_data.show()

    spark.stop()
  }

}