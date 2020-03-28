package MapReduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import sun.jvm.hotspot.debugger.Page;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The type Page ranking.
 */
public class PageRanking {
    private static String queryText="";
    private String queryFile = "src/main/resources/temp_query.txt";


    /**
     * Query input.
     *
     * @throws IOException the io exception
     */
    public void QueryInput() throws IOException {
        System.out.println("Query:");
        //BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        //String s = br.readLine();
        String s = "wotanism details troth";
        this.queryText = s;
        StringTokenizer itr = new StringTokenizer(this.queryText);


        BufferedWriter writer = new BufferedWriter(new FileWriter(this.queryFile));
        while(itr.hasMoreTokens()){
            writer.write(itr.nextToken()+"\n");
        }
        writer.close();
    }


    /**
     * The type Bm 25 mapper 1.
     * <p>
     * Handle the Term Occurrence File
     */
    public static class BM25Mapper1 extends Mapper<LongWritable, Text, Text, Text>{
        /**
         * The Query.
         */
        public String[] query;
        /**
         * The Query set.
         */
        public Set<String> querySet;
        public void setup(Context context){
            Configuration conf = context.getConfiguration();
            String temp_query = conf.get("query");
            this.query = temp_query.split("\\s+");
            this.querySet = new HashSet<>(Arrays.asList(this.query));
        }
        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException{
            String line = lineText.toString();
            line = line.trim();
            String[] k_v = line.split("\t");
            String[] term_doc = k_v[0].split("-");
            String term = term_doc[0];
            String doc = term_doc[1];
            if (this.querySet.contains(term)){
                context.write(new Text(doc+"-"+term), new Text("OCC-"+k_v[1]));
            }
        }
    }

    /**
     * The type Bm 25 mapper 2.
     *
     * For Document Coefficient
     */
    public static class BM25Mapper2 extends Mapper<LongWritable, Text, Text, Text>{
        /**
         * The Query.
         */
        public String[] query;
        /**
         * The Query set.
         */
        public Set<String> querySet;
        public void setup(Context context){
            Configuration conf = context.getConfiguration();
            String temp_query = conf.get("query");
            this.query = temp_query.split("\\s+");
            this.querySet = new HashSet<>(Arrays.asList(this.query));
        }
        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException{
            String line = lineText.toString();
            line = line.trim();
            String[] k_v = line.split("\t");
            String doc = k_v[0];
            String coef = k_v[1];
            for(String item : query){
                context.write(new Text(doc+"-"+item), new Text("COE-"+coef));
            }
        }
    }

    public static class BM25Mapper3 extends Mapper<LongWritable, Text, Text, Text>{
        /**
         * The Query.
         */
        public String[] query;
        /**
         * The Query set.
         */
        public Set<String> querySet;
        public void setup(Context context){
            Configuration conf = context.getConfiguration();
            String temp_query = conf.get("query");
            this.query = temp_query.split("\\s+");
            this.querySet = new HashSet<>(Arrays.asList(this.query));
        }
        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException{
            String line = lineText.toString();
            line = line.trim();
            String[] k_v = line.split("\t");
            String[] term_doc = k_v[0].split("-");
            String term = term_doc[0];
            String doc = term_doc[1];
            if (this.querySet.contains(term)){
                context.write(new Text(doc+"-"+term), new Text("IDF-"+k_v[1]));
            }
        }
    }

    public static class BM25Reducer extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text doc_term, Iterable<Text> args, Context context)
                throws IOException, InterruptedException{
            int occ = 0;
            float coe = (float) 0.0;
            float idf = (float) 0.0;
            for(Text arg:args){
                String line = arg.toString();
                String type = line.split("-")[0];
                if(type.equals("OCC")){
                    occ=Integer.parseInt(line.split("-")[1]);
                }
                else if(type.equals("COE")){
                    coe=Float.parseFloat(line.split("-")[1]);
                }
                else{
                    idf=Float.parseFloat(line.split("-")[1]);
                }
            }
            float bm25 = (float)2.5*occ*idf/(occ+coe);
            String doc = doc_term.toString().split("-")[0];
            context.write(new Text(doc), new Text(String.valueOf(bm25)));
        }
    }
/*
    public static class BM25Reducer extends Reducer<Text, FloatWritable, Text, FloatWritable>{
        public void reduce(Text doc, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            float sum = (float) 0.0;
            for(Text value:values){
                sum+=Float.parseFloat(value.toString());
            }
            context.write(new Text(doc), new FloatWritable(sum));
        }
    }
*/

    public static class BM25Mapper extends Mapper<LongWritable, Text, Text, FloatWritable>{
        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException{
            String line = lineText.toString();
            String[] k_v = line.split("\t");
            String doc = k_v[0];
            context.write(new Text(doc), new FloatWritable(Float.parseFloat(k_v[1])));
        }
    }

    public static class BM25ConReducer extends Reducer<Text, FloatWritable, Text, FloatWritable>{
        public void reduce(Text doc, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException{
            float sum = (float) 0.0;
            for(FloatWritable value:values){
                sum+=value.get();
            }
            context.write(new Text(doc), new FloatWritable(sum));
        }
    }
    /**
     * Online run.
     *
     * @param args the args
     * @throws IOException the io exception
     */
    public static void onlineRun(String [] args)
            throws IOException, ClassNotFoundException, InterruptedException {
        /**
         * Stage 1: BM25 Calculation
         */

        String s1_output_dir = args[1]+"/bm25_raw";
        String input_dir1 = "/Users/meow/Documents/Projects/UoG_S2/BD/Report/Data/output/output_20/2tf/part-r-00000";
        String input_dir2 = "/Users/meow/Documents/Projects/UoG_S2/BD/Report/Data/output/output_20/4coef/part-r-00000";
        String input_dir3 = "/Users/meow/Documents/Projects/UoG_S2/BD/Report/Data/output/output_20/3idf/part-r-00000";
        Configuration conf = new Configuration();
        conf.set("query", "wotanism details troth");
        Job job1 = Job.getInstance(conf, "BM25 Calculation");
        job1.setJarByClass(PageRanking.class);

        MultipleInputs.addInputPath(job1, new Path(input_dir1), TextInputFormat.class, PageRanking.BM25Mapper1.class);
        MultipleInputs.addInputPath(job1, new Path(input_dir2), TextInputFormat.class, PageRanking.BM25Mapper2.class);
        MultipleInputs.addInputPath(job1, new Path(input_dir3), TextInputFormat.class, PageRanking.BM25Mapper3.class);
        //job1.setCombinerClass(PageRanking.BM25Combiner.class);
        job1.setReducerClass(PageRanking.BM25Reducer.class);

        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(FloatWritable.class);
        // FileInputFormat.addInputPath(job, new Path(temp_file.getAbsolutePath()));

        FileOutputFormat.setOutputPath(job1, new Path(s1_output_dir));

        if (!job1.waitForCompletion(true)){
            System.exit(1);
        }
        /**
         * Stage 2: Conclusion
         */
        String s2_output_dir = args[1]+"/bm25";
        Job job2 = Job.getInstance(conf, "BM25 Conclusion");
        job2.setJarByClass(PageRanking.class);
        MultipleInputs.addInputPath(job2, new Path(s1_output_dir+"/part*"), TextInputFormat.class, PageRanking.BM25Mapper.class);
        job2.setReducerClass(PageRanking.BM25ConReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(FloatWritable.class);
        FileOutputFormat.setOutputPath(job2, new Path(s2_output_dir));
        if (!job2.waitForCompletion(true)){
            System.exit(1);
        }
    }

    /**
     * Offline run.
     *
     * @param args the args
     * @throws IOException the io exception
     */
    public static void offlineRun(String [] args) throws IOException{

    }

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws IOException the io exception
     */
//public static class PRMapper extends Mapper<>
    public static void main(String [] args)
            throws IOException, InterruptedException, ClassNotFoundException {

        //PageRanking pr = new PageRanking();
        //pr.QueryInput();

        onlineRun(args);
    }
}
