package MapReduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The type Doc tf.
 *
 * @author Molin Liu Test Class for calculating document term frequency.
 */
public class DocTF {

    /**
     * 1. Input files preprocessing stage Text Preprocessing Mapper
     */
    public static final Pattern HEAD_PATTERN = Pattern.compile("^\\[{2}.*\\]{2}");

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
                // More detail please refer to
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
            context.write(temp_title, new Text(article_body));
        }
    }

    /**
     * tpRun.
     *
     * @param args the args
     * @throws Exception the exception
     */
    public static void tpRun(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Title Extraction");
        job.setJarByClass(DocTF.class);
        job.setMapperClass(DocTF.TPMapper.class);
        // job.setCombinerClass(SplitMapReduce.SplitReducer.class);
        job.setReducerClass(DocTF.TPReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // The input is a folder.
        MultipleInputs.addInputPath(job, new Path("src/main/resources/Mockdata"), TextInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * 2. Document term frequency stage
     */

    /**
     * The type Doc tf mapper.
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
                    word.set(itr.nextToken() + "-" + key_title);
                    docLen += 1;
                    context.write(word, one);
                }
                mos.write("DocLen", new Text(key_title), new IntWritable(docLen));
            }
        }
    }

    /**
     * The type Doc tf reducer.
     */
    public static class DocTFReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {
        public void reduce(Text word, Iterable<IntWritable> counts, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable count : counts) {
                sum += count.get();
            }
            float termFreq = (float) 0.0;
            termFreq = 1 + (float) Math.log10(sum);
            context.write(word, new FloatWritable(termFreq));
        }
    }

    /**
     * Run hadoop job
     *
     * @param args the args
     * @throws Exception the exception
     */
    public static void run(String[] args) throws Exception {
        TextPreprocess tp = new TextPreprocess();
        File temp_file = tp.textCleaner(args[0]);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Title Extraction");
        job.setJarByClass(DocTF.class);
        job.setMapperClass(DocTF.DocTFMapper.class);
        // job.setCombinerClass(SplitMapReduce.SplitReducer.class);
        job.setReducerClass(DocTF.DocTFReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(temp_file.getAbsolutePath()));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * Gzip run int.
     *
     * @param args the args
     * @return the int
     * @throws Exception the exception
     */
    public static int gzipRun(String[] args) throws Exception {
        String temp_path = args[1] + "/1tp";
        // Stage 1: Input Preprocessing
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Title Extraction");
        job1.setJarByClass(DocTF.class);
        job1.setMapperClass(TPMapper.class);
        // job.setCombinerClass(SplitMapReduce.SplitReducer.class);
        job1.setReducerClass(TPReducer.class);
        // job1.setMapOutputKeyClass(Text.class);
        // job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        // FileInputFormat.addInputPath(job, new Path(temp_file.getAbsolutePath()));
        MultipleInputs.addInputPath(job1, new Path("src/main/resources/Mockdata"), TextInputFormat.class);
        FileOutputFormat.setOutputPath(job1, new Path(temp_path));
        job1.waitForCompletion(true);

        // Stage 2: TF
        String s2_outdir = args[1] + "/2tf";
        Job job2 = Job.getInstance(conf, "TF");
        job2.setJarByClass(DocTF.class);
        job2.setMapperClass(DocTFMapper.class);
        job2.setReducerClass(DocTFReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(FloatWritable.class);
        MultipleOutputs.addNamedOutput(job2, "DocLen", TextOutputFormat.class, Text.class, IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(temp_path));
        FileOutputFormat.setOutputPath(job2, new Path(s2_outdir));
        return job2.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws Exception the exception
     */
    public static void main(String[] args) throws Exception {
        gzipRun(args);
    }
}
