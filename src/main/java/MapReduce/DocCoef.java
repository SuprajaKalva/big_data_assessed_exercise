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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import javax.print.Doc;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The type Doc coef.
 *
 * @implNote The input file should be pre-processed file.
 */
public class DocCoef {
    /**
     * The constant numDoc.
     */
    public static int numDoc = 0;
    /**
     * The constant sumLen.
     */
    public static int sumLen = 0;
    /**
     * The constant avgLen.
     */
    public static float avgLen;
    /**
     * The constant HEAD_PATTERN.
     */
    public static final Pattern HEAD_PATTERN = Pattern.compile("^\\[{2}.*\\]{2}");

    /**
     * The type Doc coef mapper.
     */
    public static class DocCoefMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException{

            String line = lineText.toString();

            line = line.trim();
            if (line.equals("")) {
                return;
            }

            Matcher head_matcher = HEAD_PATTERN.matcher(line);
            if (head_matcher.find()) {
                String key_title = head_matcher.group(0);
                int docLen = line.split("\\s+").length;
                numDoc+=1;
                sumLen+=docLen;
                docLen += 1;
                context.write(new Text(key_title), new IntWritable(docLen));
            }
        }
    }

    /**
     * The type Doc coef reducer.
     */
//public static class DocCoefCombiner extends Reducer<Text, IntWritable, Text, FloatWritable>
    public static class DocCoefReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {
        //private MultipleOutputs<Text, FloatWritable> mos;
        public void setup(Context context)
                throws IOException, InterruptedException{
            avgLen = (float)sumLen/(float)numDoc;
            //mos = new MultipleOutputs<>(context);

        }
        public void reduce(Text title, Iterable<IntWritable> docLens, Context context)
                throws IOException, InterruptedException{
            //mos.write("Args", new Text("avg"), new FloatWritable(avgLen));
            //mos.write("Args", new Text("num_doc"), new FloatWritable(numDoc));
            //mos.write("Args", new Text("sumLen"), new FloatWritable(sumLen));
            float docCoe = (float) 0.375;
            for (IntWritable docLen : docLens){
                docCoe+= 1.125 * docLen.get() / avgLen;
            }
            context.write(new Text(title), new FloatWritable(docCoe));
        }
    }

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws Exception the exception
     */
    public static void main(String [] args) throws Exception{
        String input_dir = args[0];
        String output_dir = args[1];
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Document Coefficient Calculation");
        job1.setJarByClass(DocCoef.class);
        job1.setMapperClass(DocCoef.DocCoefMapper.class);
        // job.setCombinerClass(SplitMapReduce.SplitReducer.class);
        job1.setReducerClass(DocCoef.DocCoefReducer.class);
        // job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(FloatWritable.class);
        // FileInputFormat.addInputPath(job, new Path(temp_file.getAbsolutePath()));
        MultipleOutputs.addNamedOutput(job1, "Args", TextOutputFormat.class, Text.class, FloatWritable.class);
        MultipleInputs.addInputPath(job1, new Path(input_dir), TextInputFormat.class);
        FileOutputFormat.setOutputPath(job1, new Path(output_dir));

        if (!job1.waitForCompletion(true)){
            System.exit(1);
        }
    }
}
