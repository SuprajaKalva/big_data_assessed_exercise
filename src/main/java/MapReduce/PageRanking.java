package MapReduce;

import org.apache.hadoop.io.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * PageRanking Class
 *
 * Take the result of DataPreprocess.java as input.
 *
 * Stage 1: BM25 Calculation
 *      Mappers:
 *          BM25Mapper1 takes Term Occurrence file as input;
 *          BM25Mapper2 takes Document Coefficient file as input;
 *          BM25Mapper3 takes IDF file as input;
 *      Reducer:
 *          BM25Reducer will handle all k-v pairs above to calculate
 *          the BM25 score for single query term.
 * Stage 2: BM25 Conclusion
 *      Mapper:
 *          BM25Mapper takes the BM25Reducer's result as input
 *      Reducer:
 *          BM25ConReducer sums up the scores for each document.
 *
 */
public class PageRanking {
    private static String queryText="";
    /**
     * Query input.
     *
     * @throws IOException the io exception
     */
    public void QueryInput() throws IOException {
        System.out.println("Query:");
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String s = br.readLine();
        //String s = "wotanism details troth";
        this.queryText = s;
    }

    public static class BM25Pair implements WritableComparable {
        private String title;
        private Float bm25;

        public Float getBm25() {
            return bm25;
        }

        public String getTitle() {
            return title;
        }

        public void setBm25(Float bm25) {
            this.bm25 = bm25;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        @Override
        public int compareTo(Object o) {
            BM25Pair temp = (BM25Pair) o;
            return this.bm25.compareTo(temp.bm25);
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {

        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {

        }
    }
    /**
     * The type Bm 25 mapper 1.
     *
     * For the Term Occurrence File
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

        /**
         * Read the configuration, which contains the query terms.
         * @param context
         */
        public void setup(Context context){

            Configuration conf = context.getConfiguration();
            String temp_query = conf.get("query");
            this.query = temp_query.split("\\s+");
            this.querySet = new HashSet<>(Arrays.asList(this.query));
        }

        /**
         * Emit <doc-term, OCC-occurrence>
         * @param offset
         * @param lineText
         * @param context
         * @throws IOException
         * @throws InterruptedException
         *
         */
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

        /**
         * Emit <doc-term, COE-coefficient>
         * @param offset
         * @param lineText
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException{
            String line = lineText.toString();
            line = line.trim();
            String[] k_v = line.split("\t");
            String doc = k_v[0];
            String coef = k_v[1];
            for(String term : query){
                context.write(new Text(doc+"-"+term), new Text("COE-"+coef));
            }
        }
    }

    /**
     * The type Bm 25 mapper 3.
     */
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
            if(line.equals("")){
                return;
            }
            String[] k_v = line.split("\t");
            if(k_v.length==2){
                try{
                    String[] term_doc = k_v[0].split("-");
                    String term = term_doc[0];
                    String doc = term_doc[1];
                    if (this.querySet.contains(term)){
                        context.write(new Text(doc+"-"+term), new Text("IDF-"+k_v[1]));
                    }
                }
                catch (Exception e){
                    System.out.println(line);
                    e.printStackTrace();
                }

            }

        }
    }

    /**
     * Stage 1
     * The type Bm 25 reducer.
     */
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

    /**
     * Stage 2
     * The type Bm 25 mapper.
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

    /**
     * Stage 2
     * The type Bm 25 con reducer.
     */
    public static class BM25ConReducer extends Reducer<Text, FloatWritable, FloatWritable, Text>{
        public void reduce(Text doc, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException{
            float sum = (float) 0.0;
            for(FloatWritable value:values){
                sum+=value.get();
            }
            context.write(new FloatWritable(sum), new Text(doc));
        }
    }
    public static class BM25KeyComparator extends WritableComparator{
        public BM25KeyComparator(){
            super(FloatWritable.class, true);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
            Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();

            return v1.compareTo(v2) * (-1);
        }
    }
    public static class BM25Comparator extends WritableComparator {

        public BM25Comparator() {
            super(BM25Pair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            BM25Pair temp1 = (BM25Pair) a;
            BM25Pair temp2 = (BM25Pair) b;
            return temp1.compareTo(temp2);
            //return super.compare(a, b);
        }
        /*
        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {

            Float v1 = ByteBuffer.wrap(b1, s1, l1).getFloat();
            Float v2 = ByteBuffer.wrap(b2, s2, l2).getFloat();

            return v1.compareTo(v2) * (-1);
        }
        */
    }


    public static class BM25SortMapper extends Mapper<LongWritable, Text, BM25Pair, IntWritable>{
        private NullWritable nullValue = NullWritable.get();
        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            String line = lineText.toString().trim();
            String [] temp_value = line.split("\t");
            BM25Pair temp_pair = new BM25Pair();
            temp_pair.setTitle(temp_value[0]);
            temp_pair.setBm25(Float.parseFloat(temp_value[1]));
            context.write(temp_pair, new IntWritable(0));
        }
    }

    public static class BM25SortReducer extends Reducer<BM25Pair, NullWritable, Text, Text>{
        public void reduce(BM25Pair pair, Iterable<NullWritable> nullWritables, Context context)
                throws IOException, InterruptedException {
            context.write(new Text(pair.getTitle()), new Text(pair.getBm25().toString()));
        }
    }
    /**
     * Online run.
     * Calculate the query terms' BM25 scores.
     * @param args the args
     * @throws IOException            the io exception
     * @throws ClassNotFoundException the class not found exception
     * @throws InterruptedException   the interrupted exception
     */
    public static void onlineRun(String [] args)
            throws IOException, ClassNotFoundException, InterruptedException {

        /**
         * Get input from user
         */
        System.out.println("Query:");
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String s = br.readLine();
        s = s.toLowerCase();

        /**
         * Stage 1: BM25 Calculation
         */

        String input_dir = args[0];
        String s1_output_dir = args[1]+"/bm25_raw";

        String input_dir1 = input_dir+"/2tf/part-r-00000";
        String input_dir2 = input_dir+"/4coef/part-r-00000";
        String input_dir3 = input_dir+"/3idf/part-r-00000";
        Configuration conf = new Configuration();
        conf.set("query", s);
        Job job1 = Job.getInstance(conf, "BM25 Calculation");
        job1.setJarByClass(PageRanking.class);

        /**
         * Set mappers to different input files
         */
        MultipleInputs.addInputPath(job1, new Path(input_dir1), TextInputFormat.class, PageRanking.BM25Mapper1.class);
        MultipleInputs.addInputPath(job1, new Path(input_dir2), TextInputFormat.class, PageRanking.BM25Mapper2.class);
        MultipleInputs.addInputPath(job1, new Path(input_dir3), TextInputFormat.class, PageRanking.BM25Mapper3.class);

        //I'd really like to use combiner to do the rest job. However I didn't make it :c
        //job1.setCombinerClass(PageRanking.BM25Combiner.class);
        job1.setReducerClass(PageRanking.BM25Reducer.class);

        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(FloatWritable.class);

        FileOutputFormat.setOutputPath(job1, new Path(s1_output_dir));

        if (!job1.waitForCompletion(true)){
            System.exit(1);
        }
        /*
         * Stage 2: Conclusion
         */
        String s2_output_dir = args[1]+"/bm25";
        Job job2 = Job.getInstance(conf, "BM25 Conclusion");
        job2.setJarByClass(PageRanking.class);
        MultipleInputs.addInputPath(job2, new Path(s1_output_dir+"/part*"), TextInputFormat.class, PageRanking.BM25Mapper.class);
        job2.setReducerClass(PageRanking.BM25ConReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(FloatWritable.class);

        job2.setOutputKeyClass(FloatWritable.class);
        job2.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job2, new Path(s2_output_dir));
        if (!job2.waitForCompletion(true)){
            System.exit(1);
        }
        /*
         * Stage 3: Sorting
         */
        String s3_output_dir = args[1]+"/sorted";
        Job job3 = Job.getInstance(conf, "Sorting");
        job3.setJarByClass(PageRanking.class);
        MultipleInputs.addInputPath(
                job3,
                new Path(s2_output_dir+"/part*"),
                TextInputFormat.class,
                PageRanking.BM25SortMapper.class);
        job3.setSortComparatorClass(BM25Comparator.class);
        //job3.setMapperClass(PageRanking.BM25SortMapper.class);
        job3.setReducerClass(PageRanking.BM25SortReducer.class);
        job3.setOutputValueClass(Text.class);
        job3.setMapOutputValueClass(NullWritable.class);
        FileOutputFormat.setOutputPath(job3, new Path(s3_output_dir));
        job3.setNumReduceTasks(1);
/*
        if (!job3.waitForCompletion(true)){
            System.exit(1);
        }
        */
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
     * @throws IOException            the io exception
     * @throws InterruptedException   the interrupted exception
     * @throws ClassNotFoundException the class not found exception
     */
    public static void main(String [] args)
            throws IOException, InterruptedException, ClassNotFoundException {
        onlineRun(args);
    }
}
