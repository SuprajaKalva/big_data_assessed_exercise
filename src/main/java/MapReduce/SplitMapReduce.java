package MapReduce;

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

public class SplitMapReduce {
    public static final Pattern HEAD_PATTERN = Pattern.compile("^(\\[){2}.*(\\]){2}");

    public static class SplitMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
            String line = lineText.toString();
            List<Text> allTitles = new ArrayList<Text>();
            Matcher m = HEAD_PATTERN.matcher(line);
            while (m.find()) {
                allTitles.add(new Text(m.group()));
            }
            String[] allContent = HEAD_PATTERN.split(line);
            for (int i = 0; i < allTitles.size(); i++) {
                Text contentValue = new Text(allContent[i]);
                context.write(allTitles.get(i), contentValue);
            }
        }
    }

    public static class SplitReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text word, NullWritable Null, Context context) throws IOException, InterruptedException {
            context.write(word, null);
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Title Extraction");
        job.setJarByClass(SplitMapReduce.class);
        job.setMapperClass(SplitMapper.class);
        job.setCombinerClass(SplitMapReduce.SplitReducer.class);
        job.setReducerClass(SplitMapReduce.SplitReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(null);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}