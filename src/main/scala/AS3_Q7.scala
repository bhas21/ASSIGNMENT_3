import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window


object AS3_Q7 {
  def main(Args:Array[String]){
    //setting the logging level
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("Bhaskar")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._ // for toDF function

    val data=List(
      ( "V001", "P001", "2023-11-01", "Dr. Smith", 700, "Cardiology"),
      ("V002", "P002", "2023-11-10", "Dr. Johnson", 400, "Neurology"),
      ("V003", "P003", "2023-12-01", "Dr. Brown", 900, "Cardiology"),
      ("V004", "P004", "2023-12-15", "Dr. Smith", 600, "Cardiology"),
      ("V005", "P005", "2024-01-01", "Dr. Johnson", 450, "Neurology")
    ).toDF("visit_id", "patient_id", "visit_date", "doctor_name", "treatment_cost" ,"department")

    val new_data=data.withColumn("Visit_Year",year($"visit_date"))

    new_data.show()

    val filter_data=data.filter(col("department").startsWith("Cardio")
      && col("treatment_cost")>500)

    filter_data.show()

    val groupped_data=new_data.groupBy("doctor_name","Visit_Year").
      agg(sum(col("treatment_cost")).alias("Total_Treatment_Cost"),
        max(col("treatment_cost")).alias("Max_Treatment_Cost"),
        count(col("doctor_name")).alias("Doctor's_Count")
      )

    groupped_data.show()

    val window=Window.partitionBy("patient_id").orderBy("visit_date")

    val lead_data=data.select(col("*"),
      lead(col("treatment_cost"),1).over(window).alias("Next_Treatment_Cost"))

    lead_data.show()

    spark.stop()
  }

}