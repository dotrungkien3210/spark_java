package ex1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Ingesting_CSV {
    public static void main(String[] args) {
        Ingesting_CSV app = new Ingesting_CSV();
        app.start();
    }
    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("CSV to Dataset")
                .master("local")
                .getOrCreate();
        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .load("data/books.csv");
        df.show(5);
    }
}
