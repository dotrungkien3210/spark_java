package lab100;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Computes Pi.
 *
 * @author jgp
 */
public class pi_compute implements Serializable {
    private static final long serialVersionUID = -1546L;
    private static long counter = 0;

    /**
     * Mapper class, creates the map of dots
     *
     * @author jgp
     */
    // hàm call ở MapFunction<Row, Integer> sẽ nhận row và trả về integer
    private final class DartMapper
            implements MapFunction<Row, Integer> {
        private static final long serialVersionUID = 38446L;

        @Override
        public Integer call(Row r) throws Exception {
            double x = Math.random() * 2 - 1;
            double y = Math.random() * 2 - 1;
            counter++;
            if (counter % 100000 == 0) {
                System.out.println("" + counter + " darts thrown so far");
            }
            return (x * x + y * y <= 1) ? 1 : 0;
        }
    }

    /**
     * Reducer class, reduces the map of dots
     *
     * @author jgp
     */
    private final class DartReducer implements ReduceFunction<Integer> {
        private static final long serialVersionUID = 12859L;

        @Override
        public Integer call(Integer x, Integer y) {
            return x + y;
        }
    }

    /**
     * main() is your entry point to the application.
     *
     * @param args
     */
    public static void main(String[] args) {
        pi_compute app = new pi_compute();
        app.start(10);
    }

    /**
     * The processing code.
     */
    private void start(int slices) {
        int numberOfThrows = 100000 * slices;
        System.out.println("About to throw " + numberOfThrows
                + " darts, ready? Stay away from the target!");
        // tạo mội session mới
        long t0 = System.currentTimeMillis();
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Pi")
                .master("local[*]")
                .getOrCreate();

        long t1 = System.currentTimeMillis();
        System.out.println("Session initialized in " + (t1 - t0) + " ms");
        // tạo một list các lần ném mà ở đây là 1 triệu lần
        // output của cái này là [1,2,3,4...,1.000.000]
        List<Integer> l = new ArrayList<>(numberOfThrows);
        for (int i = 0; i < numberOfThrows; i++) {
            l.add(i);
        }
        // chuyển đổi từ list thành dataset rồi thành dataframe
        // khi chuyển từ list sang dataset ta cần cung cấp kiểu dữ liệu
        // kiểu dữ liệu ở đây là dạng int
        Dataset<Row> incrementalDf = spark
                .createDataset(l, Encoders.INT())
                .toDF();

        long t2 = System.currentTimeMillis();
        System.out.println("Initial dataframe built in " + (t2 - t1) + " ms");
        // tạo một dataset mới với dữ liệu trả về từ phía hàm DartMapper
        // output của hàm là [0,1,1,0,1...] chính là kết quả mỗi lần ném
        // ném ngoài hình tròn trả về 0 ném trong hình tròn trả về 1
        Dataset<Integer> dartsDs = incrementalDf
                .map(new DartMapper(), Encoders.INT());
        dartsDs.show(5);

        long t3 = System.currentTimeMillis();
        System.out.println("Throwing darts done in " + (t3 - t2) + " ms");

        // .reduce sẽ thực hiện cộng tất cả
        int dartsInCircle = dartsDs.reduce(new DartReducer());
        long t4 = System.currentTimeMillis();
        System.out.println("Analyzing result in " + (t4 - t3) + " ms");

        System.out.println("Pi is roughly " + 4.0 * dartsInCircle / numberOfThrows);

        spark.stop();
    }
}
