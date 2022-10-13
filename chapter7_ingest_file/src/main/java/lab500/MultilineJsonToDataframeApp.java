package lab500;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Multiline ingestion JSON ingestion in a dataframe.
 * ở đây là chỉ ra error nếu như json có nhiều dòng
 * Exception in thread "main" org.apache.spark.sql.AnalysisException: Since Spark 2.3, the queries from raw JSON/CSV files
 * are disallowed when the referenced columns only include the internal corrupt record column
 *
 * @author jgp
 */
public class MultilineJsonToDataframeApp {

    /**
     * main() is your entry point to the application.
     *
     * @param args
     */
    public static void main(String[] args) {
        MultilineJsonToDataframeApp app =
                new MultilineJsonToDataframeApp();
        app.start();
    }

    /**
     * The processing code.
     */
    private void start() {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Multiline JSON to Dataframe")
                .master("local")
                .getOrCreate();

        // Reads a JSON, called countrytravelinfo.json, stores it in a dataframe
        Dataset<Row> df = spark.read()
                .format("json")
                .option("multiline", true)
                .load("data/countrytravelinfo.json");


        // Shows at most 3 rows from the dataframe
        df.show(3);
        df.printSchema();
    }
}
