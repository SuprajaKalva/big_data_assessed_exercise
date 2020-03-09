import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.conf.Configuration;
import scala.Tuple2;
import scala.xml.dtd.impl.WordExp;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Sample2 {
/*

JUST THOUGHTS!!!

@TODO: Pre-Processing - Document CleanUp

First Map (doc_id, document_info) - get each record using delimiter
Second Map (doc_id, [words_in_document]) - convert each record into a list of words
Third Map (doc_id, [words_in_document_without_stopwords])
Fourth Map (doc_id, [words_in_document_without_punctuation])

Final RDD --> (doc_id, [words_in_document])

@TODO: Calculate Term Frequency
Take each word in the document and calculate how many time it occurs in the document
USE WORD COUNT AS AN EXAMPLE

@TODO: Calculate Document Frequency
groupBy(term).agg(countDistinct)?

@TODO: Calculate Inverse Document Frequency
create a function and calculate this

@TODO: Calculate TF_IDF
Multiply the term frequencies with inverse document frequencies

 */



    private static final PairFunction<Text, String, ArrayList<String>> WORDS_MAPPER =
            (PairFunction<Text, String, ArrayList<String>>) text -> {
                String s = text.toString();
                return new Tuple2<>(s, token_creator(s));
            };

    public static ArrayList<String> loadStopwords() throws IOException {
        Scanner s = new Scanner(new File("trialOne/src/main/resources/stopwords_list.txt"));
        ArrayList<String> stopwords = new ArrayList<>();
        while (s.hasNext()){
            stopwords.add(s.next());
        }
        s.close();
        return stopwords;
    }


    public static ArrayList<String> token_creator(String s) throws IOException {
        ArrayList<String> allWords = Stream.of(s.toLowerCase().split(" "))
                .collect(Collectors.toCollection(ArrayList::new));

        allWords.forEach(word -> word.replaceAll("\\p{Punct}",""));
        allWords.removeAll(loadStopwords());

//        assertEquals(result, target);

        return allWords;
    }

//    private static final Function2<ArrayList<String>, ArrayList<String>, Integer> WORDS_REDUCER =
//            new Function2<ArrayList<String>, ArrayList<String>, Integer>() {
//                @Override
//                public Integer call(ArrayList<String> a, Integer b) throws Exception {
//                    return a + b;
//                }
//            };


    public static void main(String[] args) {

        final int[] countOfRecords = {0};
        SparkConf conf = new SparkConf().setAppName("Example").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        Configuration hadoopConf = new Configuration();

        // [[ is the record separator
        hadoopConf.set("textinputformat.record.delimiter", "[[");
        JavaPairRDD<String, ArrayList<String>> rdd = jsc.newAPIHadoopFile("trialOne/src/main/resources/SampleText2.txt",
                TextInputFormat.class, LongWritable.class, Text.class, hadoopConf).values().mapToPair(WORDS_MAPPER);


        // INPUT: rdd -> ( String doc, ArrayList<String> terms)

        //@TODO: FIRST MAP PHASE ( (String doc, ArrayList<String>  terms), 1) )

        //@TODO: FIRST REDUCE PHASE ( (String doc, String term), TF)


        //@TODO: SECOND MAP PHASE ( String term, 1 ) -> When TF > 0

        //@TODO: SECOND REDUCE PHASE ( String term, DF )




        //@TODO: FIRST MAP PHASE ( (Integer doc_id, ArrayList<String>  terms), 1) )

        //@TODO: FIRST REDUCE PHASE ( (Integer doc_id, String term), TF)


        //@TODO: SECOND MAP PHASE ( String term, 1 ) -> When TF > 0

        //@TODO: SECOND REDUCE PHASE ( String term, DF )

        System.out.println(rdd.collect());

//        .map(new Function<Text, String>() {
//
//            @Override
//            public String call(Text text) throws Exception {
//                return text.toString();
//            }});



//        for(String document:rdd.collect()) {
//            System.out.println(" NEW!!! " + document);
//        }



//        rdd.foreach(new VoidFunction(){
//
//            @Override
//            public void call(Object record) throws Exception {
//                System.out.println("New Record" + countOfRecords[0]);
//                countOfRecords[0]++;
//
//
//            }});

//        System.out.println(rdd.count());
//        System.out.println(rdd.collect());

//        JavaRDD<String> n_rdd = rdd.flatMap(new FlatMapFunction<String, String>() {
//
//                @Override
//                public Iterator<String> call(String s) throws Exception {
//                    return Arrays.asList(s.split(" ")).iterator();
//                }
//        });

        rdd.saveAsTextFile("EachDoc");
//        n_rdd.saveAsTextFile("EachWord");


    }
}