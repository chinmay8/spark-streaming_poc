import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_date, datediff, lit, when}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types._

object StreamClass {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .master("local")
      .appName("example")
      .config("spark.sql.warehouse.dir", "C:\\Users\\Chinmay\\IdeaProjects\\Spark_Aurora_POC\\warehouse_dir")
      .getOrCreate()
//job|company|blood_group|username|name|mail|birthdate|salary|id|gender
    val schema = StructType(
      Array(StructField("job", StringType),
        StructField("company", StringType),
        StructField("blood_group", StringType),
        StructField("username", StringType),
        StructField("name", StringType),
        StructField("mail", StringType),
        StructField("birthdate", StringType),
        StructField("salary", IntegerType),
        StructField("id", IntegerType),
        StructField("gender", StringType)))

    //create stream from folder
    val fileStreamDf = sparkSession.readStream
      .option("header", "true")
      .option("sep","|")
      .schema(schema)
      .csv("C:\\Users\\Chinmay\\IdeaProjects\\StreamingDemo_Maven\\input")
    //    val inputdf = sparkSession.readStream.format("socket").option("host","localhost").option("port","8080").load()

    val enrichDf = fileStreamDf.
      withColumn("gender", when(col("gender")==="M",lit("Male"))
        .when(col("gender")==="F",lit("Female"))
      .otherwise(lit("N/A")))
      .withColumn("current_date",current_date)
      .withColumn("Age",(datediff(col("current_date"),col("birthdate"))/365).cast(IntegerType))

    val filterDf = enrichDf
      .drop("current_date")
      .filter(col("salary")>=50000)

    val query = filterDf.writeStream
      .format("console")
      .outputMode(OutputMode.Append()).start()
    //    val query = inputdf.writeStream.format("console").outputMode("append").start()

    query.awaitTermination(timeoutMs = 50000)
  }
}
