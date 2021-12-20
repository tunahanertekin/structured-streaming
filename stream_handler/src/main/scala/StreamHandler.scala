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
    import scala.reflect.runtime.universe.{TypeTag, typeTag}

    def toTuple2[S: TypeTag, T: TypeTag] = 
        udf[(S, T), S, T]((x: S, y: T) => (x, y))

    def toTuple3[S: TypeTag, T: TypeTag, F: TypeTag] = 
        udf[(S, T, F), S, T, F]((x: S, y: T, z: F) => (x, y, z))

    def toTuple4[S: TypeTag, T: TypeTag, F: TypeTag, K: TypeTag] = 
        udf[(S, T, F, K), S, T, F, K]((x: S, y: T, z: F, t: K) => (x, y, z, t))

    def toTuple6[S: TypeTag, T: TypeTag, F: TypeTag, K: TypeTag, M: TypeTag, N: TypeTag] = 
        udf[(S, T, F, K, M, N), S, T, F, K, M, N]((x: S, y: T, z: F, t: K, a: M, b: N) => (x, y, z, t, a, b))
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

        // ----- TASK 1 -----

        // val overallStatusDF = expandedDF
        //     .filter("status == 'done'")
        //     .groupBy("airport")
        //     .agg(avg("priority").as("avgPriority"), avg($"finishingTime"- $"startingTime").as("avgElapsedTime"),  count("*").as("total"))

        // val query = overallStatusDF
        //     .writeStream
        //     .outputMode("complete")
        //     .format("console")
        //     .start()


        // ----- TASK 2 -----


        val failedTasksDF = expandedDF
            .filter("status == 'failed'")

        val query = failedTasksDF
            .writeStream
            .outputMode("update")
            .format("console")
            .start()

        val failedToKafka = failedTasksDF.withColumn(
            "value", TupleUDFs.toTuple6[String, String, String, String, String, String].apply($"taskId", $"airport", $"fleet", $"robot", $"task", $"status")
        )

        val ds = failedToKafka
            .selectExpr("CAST(value AS STRING)")
            .writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("topic", "batch")
            .option("checkpointLocation", "/tmp/tunomeister/checkpoint")
            .start()
            
        // val query = overallStatusDF
        //     .writeStream
        //     .outputMode("complete")
        //     .format("console")
        //     .start()
        
        query.awaitTermination()
    }

}