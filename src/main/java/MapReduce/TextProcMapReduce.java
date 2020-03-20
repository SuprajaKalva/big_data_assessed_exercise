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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.graalvm.compiler.core.common.type.ArithmeticOpTable;

/**
 * Test class for text preprocessing
 * Input gzip type files.
 */
public class TextProcMapReduce {
    /**
     * Text Preprocessing Mapper
     * @author Molin Liu
     */

    public static final Pattern titlePattern = Pattern.compile("^\\[{2}.*\\]{2}");

    public static class TPMapper extends Mapper<LongWritable, Text, Text, Text>{
        private static String temp_title;
        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException{
            String line = lineText.toString();
            // Preprocess the input text file.
            line = line.trim();
            line = line.replaceAll("={2}.*={2}", "");
            line = line.replaceAll("[^A-Za-z0-9\\[\\]]"," ");
            Matcher titleMatcher = titlePattern.matcher(line);
            if(line.equals("")){
                return;
            }
            if(!titleMatcher.find()){
                context.write(new Text(temp_title), new Text(line));
            }else{
                temp_title = line;
            }
        }
    }

    public static class TPReducer extends Reducer<Text, Text, Text, Text>{

        public void reduce(Text temp_title, Iterable<Text> bodys, Context context)
                throws IOException, InterruptedException{
            String article_body = "";
            for(Text body: bodys){
                article_body += body.toString();
            }
            context.write(temp_title, new Text(article_body));
        }
    }
    public static void run(String [] args)
            throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Title Extraction");
        job.setJarByClass(TextProcMapReduce.class);
        job.setMapperClass(TextProcMapReduce.TPMapper.class);
        // job.setCombinerClass(SplitMapReduce.SplitReducer.class);
        job.setReducerClass(TextProcMapReduce.TPReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path("src/main/resources/Mockdata_tiny"), TextInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
