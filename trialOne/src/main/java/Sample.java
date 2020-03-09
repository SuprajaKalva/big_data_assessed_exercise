// Boilerplate Code

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

public class Sample {

    private static void wordCount(String fileName) {

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("JD Word Counter");

//        sparkConf.set("textinputformat.record.delimiter", "[[");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        Configuration hadoopConf = new Configuration();

        // set the [[ as the record seperator
        hadoopConf.set("textinputformat.record.delimiter", "[[");

//        JavaRDD<String> inputFile = sparkContext.textFile(fileName);
        JavaRDD<String> rdd = sparkContext.newAPIHadoopFile(fileName, TextInputFormat.class, LongWritable.class, Text.class, hadoopConf).values().map((Function<Text, String>) Text::toString);

        rdd.foreach((VoidFunction) o -> System.out.println("Record==>" + o));

//        JavaRDD<String> wordsFromFile = inputFile.flatMap(content -> Arrays.asList(content.split(" ")).iterator());
//
//        JavaPairRDD countData = wordsFromFile.mapToPair(t -> new Tuple2(t, 1)).reduceByKey((x, y) -> (int) x + (int) y);

        rdd.saveAsTextFile("CountData");
    }

    public static void main(String[] args) {

        if (args.length == 0) {
            System.out.println("No files provided.");
            System.exit(0);
        }

        wordCount(args[0]);
    }
}