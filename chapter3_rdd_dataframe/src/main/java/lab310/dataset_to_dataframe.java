package lab310;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Converts an array to a Dataframe via a Dataset
 *
 * @author jgp
 */
public class dataset_to_dataframe {

    public static void main(String[] args) {
        dataset_to_dataframe app = new dataset_to_dataframe();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Array to dataframe")
                .master("local")
                .getOrCreate();

        String[] stringList =
                new String[] { "Jean", "Liz", "Pierre", "Lauric" };
        List<String> data = Arrays.asList(stringList);
        Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
        ds.show();
        ds.printSchema();

        Dataset<Row> df = ds.toDF();
        df.show();
        df.printSchema();
    }
}
