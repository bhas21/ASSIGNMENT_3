import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window


object AS3_Q10 {
  def main(Args:Array[String]){
    //setting the logging level
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("Bhaskar")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._ // for toDF function
    val data=List(
      ("F001", "Delta", "2023-11-01 08:00", "2023-11-01 10:00", 40, "New York"),
      ("F002", "United", "2023-11-01 09:00", "2023-11-01 11:30", 20, "New Orleans"),
      ("F003", "American", "2023-11-02 07:30", "2023-11-02 09:00", 60, "New York"),
      ("F004", "Delta", "2023-11-02 10:00", "2023-11-02 12:15", 30, "Chicago"),
      ("F005", "United", "2023-11-03 08:45", "2023-11-03 11:00", 50, "New York")
    ).toDF("flight_id", "airline", "departure_time", "arrival_time", "delay", "destination")

    //val filtered_data=data.withColumn("delay_minutes",unix_timestamp($arrival_time)-
    // unix_timestamp($departure_time))

  }

}