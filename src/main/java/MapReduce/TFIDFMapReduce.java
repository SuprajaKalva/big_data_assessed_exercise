package MapReduce;
import java.io.File;
import java.io.IOException;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Molin Liu
 * Test class for TF-IDF calculation.
 * 2 MapReduces in this class: TF, and IDF
 */
public class TFIDFMapReduce {
    /**
     * The constant HEAD_PATTERN.
     */
    public static final Pattern HEAD_PATTERN = Pattern.compile("^(\\[){2}.*(\\]){2}");

    /**
     * The type Tf mapper.
     */
    public static class TFMapper
            extends Mapper<LongWritable, Text, Text, IntWritable>{
        private Text word = new Text();
        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
            String line = lineText.toString();
            Matcher head_matcher = HEAD_PATTERN.matcher(line);
            if (head_matcher.find()) {
                String key_title = head_matcher.group(0);
                StringTokenizer itr = new StringTokenizer(line);
                while (itr.hasMoreTokens()) {
                    word.set(itr.nextToken() + "-" + key_title);
                    context.write(word, one);
                }
            }
        }
    }

    /**
     * The type Tf reducer.
     */
    public static class TFReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {
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

    public static class IDFMapper
            extends Mapper<LongWritable, Text, Text, Text>{

        public void map(
                LongWritable offset,
                Text lineText,
                Context context)
                throws IOException, InterruptedException{
            String line = lineText.toString();
            String term_title = line.split("\t")[0];
            String termFreq = line.split("\t")[1];
            String term = term_title.split("-")[0];
            String articleTitle = term_title.split("-")[1];




        }
    }

    /**
     * Create a Term Frequency(TF) job on Hadoop
     *
     * @param args the args
     * @throws Exception the exception
     */
    public static void TFJob(String[] args) throws Exception {
        TextPreprocess tp = new TextPreprocess();
        File temp_file = tp.textCleaner(args[0]);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TF-IDF Calculation");
        job.setJarByClass(TFIDFMapReduce.class);
        job.setMapperClass(TFIDFMapReduce.TFMapper.class);
        // job.setCombinerClass(SplitMapReduce.SplitReducer.class);
        job.setReducerClass(TFIDFMapReduce.TFReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(temp_file.getAbsolutePath()));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
