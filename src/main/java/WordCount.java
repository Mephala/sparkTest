import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by mephala on 3/24/17.
 */
public class WordCount {

    private static final String OUT_FOLDER = "/home/mephala/Desktop/out";
    public static void main(String[] args) {
        // Define a configuration to use to interact with Spark
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Work Count App");

        // Create a Java version of the Spark Context from the configuration
        JavaSparkContext sc = new JavaSparkContext(conf);
        String largeFilePath = WordCount.class.getClassLoader().getResource("large.txt").getPath();
        String outputFile = OUT_FOLDER;

//        JavaRDD<String> textFile = sc.textFile("hdfs://...");
        JavaRDD<String> textFile = sc.textFile(largeFilePath);
        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        counts.saveAsTextFile(outputFile);
//        counts.saveAsTextFile("hdfs://...");

    }
}
