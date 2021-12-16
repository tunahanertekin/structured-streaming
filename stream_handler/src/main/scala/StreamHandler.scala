import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._

case class Task(
    taskId: String, 
    airport: String,
    fleet: String,
    area: String,
    robot: String,
    task: String,
    priority: Integer,
    status: String,
    startingTime: Float,
    finishingTime: Float
)

object TupleUDFs {
  import org.apache.spark.sql.functions.udf      
  // type tag is required, as we have a generic udf
  import scala.reflect.runtime.universe.{TypeTag, typeTag}

  def toTuple2[S: TypeTag, T: TypeTag] = 
    udf[(S, T), S, T]((x: S, y: T) => (x, y))
}

object StreamHandler {
    def main(args: Array[String]){
        val spark = SparkSession
            .builder
            .appName("Stream Handler")
            .getOrCreate()

        import spark.implicits._

        val inputDF = spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "assign,result")
            .load()

        val rawDF = inputDF.selectExpr("CAST(value AS STRING)").as[String]

        val expandedDF = rawDF.map(row => row.split(","))
            .map(row => Task(
                row(0),
                row(1),
                row(2),
                row(3),
                row(4),
                row(5),
                row(6).toInt,
                row(7),
                row(8).toFloat,
                row(9).toFloat
            ))


        // val summaryDf = expandedDF
		// 	.groupBy("airport")
		// 	.agg(avg("priority"))

        // val summaryDf = expandedDF
        //     .filter("task == 'Go'")

        val dfToKafka = expandedDF.withColumn(
            "value", TupleUDFs.toTuple2[String, String].apply(expandedDF("taskId"), expandedDF("status"))
        )

       

        val ds = dfToKafka
            .selectExpr("CAST(value AS STRING)")
            .writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("topic", "batch")
            .option("checkpointLocation", "/tmp/tunomeister/checkpoint")
            .start()
            
        val query = dfToKafka
            .writeStream
            .outputMode("update")
            .format("console")
            .start()
        
        query.awaitTermination()
    }

}