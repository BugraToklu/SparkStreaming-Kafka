import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class StreamingConsumer {
    public static void main(String[] args) throws StreamingQueryException {
        System.setProperty("hadoop.home.dir","C:\\hadoop-common-2.2.0-bin-master");
        SparkSession sparkSession = SparkSession.builder().appName("streaming-kafka").master("local").getOrCreate();

        StructType schema = new StructType()
                .add("product","string")
                .add("time", DataTypes.TimestampType);

        Dataset<Row> load = sparkSession.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "search")
                .load();

        Dataset<Row> data = load.selectExpr("CAST(value AS STRING) AS message")
                .select(functions.from_json(functions.col("message"), schema).alias("json"))
                .select("json.*");

        Dataset<Row> count = data.groupBy(functions.window(data.col("time"), "1 minute"), data.col("product")).count();

        StreamingQuery start = count.writeStream()
                .outputMode("update")
                .format("console")
                .start();
        start.awaitTermination();
    }
}
