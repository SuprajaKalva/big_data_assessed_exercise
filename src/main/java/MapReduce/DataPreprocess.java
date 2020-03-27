package MapReduce;

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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Data Preprocess Class
 *
 * Output file:
 * 1. Term occurrence
 * 2. Documents' length
 * 3. Term IDF
 *
 * @author Molin Liu
 */
public class DataPreprocess {

    /**
     * Text Preprocessing Mapper
     */
    public static final Pattern HEAD_PATTERN = Pattern.compile("^\\[{2}.*\\]{2}");
    /**
     * The constant num_doc.
     */
    public static int num_doc = 0;

    /**
     * Text Pre-processing Mapper Class Input type: gzip files
     */
    public static class TPMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static String temp_title;
        private static List<String> stopWordList;

        protected void setup(Context context) throws IOException, InterruptedException {
            try {
                // BufferedReader fis = new BufferedReader(new FileReader(stopWordFile_PATH));
                this.stopWordList = Files.readAllLines(Paths.get("src/main/resources/stopword-list.txt"));
            } catch (IOException ioe) {
                System.err.println("Exception while reading stop word file" + ioe.toString());
            }
        }

        public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
            String line = lineText.toString();
            // Preprocess the input text file.
            line = line.trim();
            // Remove subtitle
            line = line.replaceAll("={2}.*={2}", "");
            // Remove non-ASCII characters
            line = line.replaceAll("[^A-Za-z0-9\\[\\]]", " ");
            Matcher titleMatcher = HEAD_PATTERN.matcher(line);
            if (line.equals("")) {
                return;
            }
            // See if the current line is title:
            if (!titleMatcher.find()) {
                // Remove extra space
                line = line.replaceAll(" +", " ");
                // Remove non-title ]] symbols.
                // More detail refer to
                // https://github.com/SuprajaKalva/big_data_assessed_exercise/issues/1
                line = line.replaceAll("\\]\\]", "");

                // Remove stopwords
                List<String> allWords = new ArrayList<String>(Arrays.asList(line.toLowerCase().split(" ")));
                allWords.removeAll(stopWordList);
                line = String.join(" ", allWords);

                // Construct key-value pair.
                // Key: title of articles
                // Value: chunk of body
                context.write(new Text(temp_title), new Text(line));
            } else {
                // If it's title, simply set the current line to
                // temp_title.
                temp_title = line;
            }
        }
    }

    /**
     * Text Pre-processing Reducer Concatenate all chunks belong to the same
     * article.
     */
    public static class TPReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text temp_title, Iterable<Text> bodys, Context context)
                throws IOException, InterruptedException {
            String article_body = "";
            for (Text body : bodys) {
                if (article_body.equals("")) {
                    article_body += body.toString();
                } else {
                    article_body += " " + body.toString();
                }
            }
            num_doc += 1;
            context.write(temp_title, new Text(article_body));
        }
    }

    /**
     * The type Doc tf mapper.
     * Output the Documents' length
     */
    public static class DocTFMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text word = new Text();
        private final static IntWritable one = new IntWritable(1);
        private MultipleOutputs<Text, IntWritable> mos;

        protected void setup(Context context) {
            mos = new MultipleOutputs<>(context);
        }
        public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
            String line = lineText.toString();
            Matcher head_matcher = HEAD_PATTERN.matcher(line);
            if (head_matcher.find()) {
                String key_title = head_matcher.group(0);
                StringTokenizer itr = new StringTokenizer(line);
                int docLen = 0;
                while (itr.hasMoreTokens()) {
                    docLen += 1;
                    word.set(itr.nextToken() + "-" + key_title);
                    context.write(word, one);
                }
                mos.write("DocLen", new Text(key_title), new IntWritable(docLen));
            }
        }
    }

    /**
     * The type Doc tf reducer.
     * The output file is term occurrences in each document.
     */
    public static class DocTFReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text word, Iterable<IntWritable> counts, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable count : counts) {
                sum += count.get();
            }
            // Term occurrence
            int termOcc = sum;
            context.write(word, new IntWritable(termOcc));
        }
    }

    /**
     * The type Idf mapper.
     */
    public static class IDFMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
            String line = lineText.toString();
            String term_title = line.split("\t")[0];
            String termOcc = line.split("\t")[1];
            String term = term_title.split("-")[0];
            String articleTitle = term_title.split("-")[1];
            String term_title_tf = articleTitle + "=" + termOcc;
            context.write(new Text(term), new Text(term_title_tf));
        }
    }

    /**
     * The type Idf reducer.
     * <p>
     * Output the IDF
     */
    public static class IDFReducer extends Reducer<Text, Text, Text, FloatWritable> {
        public void reduce(Text word, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> valCache = new ArrayList<>();

            for (Text value : values) {
                valCache.add(value.toString());
            }
            float IDF;

            for (int i = 0; i < valCache.size(); i++) {
                IDF = (float) Math.log10(1 + (num_doc / valCache.size()));
                String[] article_tf = (valCache.get(i)).toString().split("=");
                context.write(new Text(word + "-" + article_tf[0]), new FloatWritable(IDF));
            }
        }
    }

    /**
     * Tfidf run.
     *
     * @param args the args
     * @throws Exception the exception
     */
    public static void Run(String[] args) throws Exception {
        String input_dir = args[0];
        String output_dir = args[1];
        String s1_outdir = output_dir + "/1tp";
        /**
         * Stage 1: Input Preprocessing
         */
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Text Preprocessing");
        job1.setJarByClass(DataPreprocess.class);
        job1.setMapperClass(DataPreprocess.TPMapper.class);
        // job.setCombinerClass(SplitMapReduce.SplitReducer.class);
        job1.setReducerClass(DataPreprocess.TPReducer.class);
        // job1.setMapOutputKeyClass(Text.class);
        // job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        // FileInputFormat.addInputPath(job, new Path(temp_file.getAbsolutePath()));
        MultipleInputs.addInputPath(job1, new Path(input_dir), TextInputFormat.class);
        FileOutputFormat.setOutputPath(job1, new Path(s1_outdir));

        if (!job1.waitForCompletion(true)){
            System.exit(1);
        }

        /**
         * Stage 2: TF
         */
        String s2_outdir = output_dir + "/2tf";
        Job job2 = Job.getInstance(conf, "TF");
        job2.setJarByClass(DataPreprocess.class);
        job2.setMapperClass(DataPreprocess.DocTFMapper.class);
        job2.setReducerClass(DataPreprocess.DocTFReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(FloatWritable.class);
        MultipleOutputs.addNamedOutput(job2, "DocLen", TextOutputFormat.class, Text.class, IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(s1_outdir+"/part*"));
        FileOutputFormat.setOutputPath(job2, new Path(s2_outdir));
        if (!job2.waitForCompletion(true)){
            System.exit(1);
        }

        /**
         * Stage 3: TF-IDF
         */
        String s3_outdir = output_dir + "/3tfidf";
        Job job3 = Job.getInstance(conf, "TF-IDF");
        job3.setJarByClass(DataPreprocess.class);
        job3.setMapperClass(DataPreprocess.IDFMapper.class);
        job3.setReducerClass(DataPreprocess.IDFReducer.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job3, new Path(s2_outdir+"/part*"));
        FileOutputFormat.setOutputPath(job3, new Path(s3_outdir));
        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws Exception the exception
     */
    public static void main(String[] args) throws Exception {
        Run(args);
    }
}
