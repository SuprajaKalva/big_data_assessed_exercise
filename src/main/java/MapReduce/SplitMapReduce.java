package MapReduce;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The type Split map reduce.
 */
public class SplitMapReduce {
    /**
     * The constant HEAD_PATTERN.
     */
    public static final Pattern HEAD_PATTERN = Pattern.compile("^(\\[){2}.*(\\]){2}");
    /**
     * The constant articlePattern.
     */
    public static final Pattern articlePattern = Pattern.compile("\\[{2}.*\\].*");

    /**
     * The type Split mapper.
     * TODO: The title extraction will not work well in some scenarios.
     */
    public static class SplitMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
            String line = lineText.toString();
            List<Text> allTitles = new ArrayList<Text>();
            Matcher m = HEAD_PATTERN.matcher(line);
            Text key_title = null;
            if (m.find()) {
                key_title = new Text(m.group(0));
                Text body = new Text(line.replaceAll("\\[{2}.*(\\]){2}", ""));
                context.write(key_title, body);
            }

        }
    }

    /**
     * The type Split combiner.
     */
    public static class SplitCombiner extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text title, Iterable<Text> bodys, Context context) throws IOException, InterruptedException {

            // context.write();
        }
    }

    /**
     * The type Split reducer.
     */
    public static class SplitReducer extends Reducer<Text, Text, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text word, Iterable<Text> bodys, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Text body : bodys) {
                sum += body.getLength();
            }
            result.set(sum);
            context.write(word, result);
        }
    }

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws Exception the exception
     */
    public static void main(String[] args) throws Exception {
        // Text preprocessing
        TextPreprocess tp = new TextPreprocess();
        File temp_file = tp.textCleaner(args[0]);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Title Extraction");
        job.setJarByClass(SplitMapReduce.class);
        job.setMapperClass(SplitMapReduce.SplitMapper.class);
        // job.setCombinerClass(SplitMapReduce.SplitReducer.class);
        job.setReducerClass(SplitMapReduce.SplitReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(temp_file.getAbsolutePath()));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}