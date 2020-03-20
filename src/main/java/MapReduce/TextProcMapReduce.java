package MapReduce;

import java.io.File;
import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.inject.internal.util.$FinalizableWeakReference;
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
     */
    public static final Pattern titlePattern = Pattern.compile("^\\[{2}.*\\]{2}");

    /**
     * Text Pre-processing Mapper Class
     * Input type: gzip files
     */
    public static class TPMapper extends Mapper<LongWritable, Text, Text, Text>{
        private static String temp_title;
        private static List<String> stopWordList;
        protected void setup(Context context)
                throws IOException, InterruptedException{
            try {
                //BufferedReader fis = new BufferedReader(new FileReader(stopWordFile_PATH));
                this.stopWordList = Files.readAllLines(Paths.get("src/main/resources/stopword-list.txt"));
            } catch (IOException ioe) {
                System.err.println("Exception while reading stop word file" + ioe.toString());
            }
        }
        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException{
            String line = lineText.toString();
            // Preprocess the input text file.
            line = line.trim();
            // Remove subtitle
            line = line.replaceAll("={2}.*={2}", "");
            // Remove non-ASCII characters
            line = line.replaceAll("[^A-Za-z0-9\\[\\]]"," "); // Added space here
            Matcher titleMatcher = titlePattern.matcher(line);
            if(line.equals("")){
                return;
            }
            // See if the current line is title:
            if(!titleMatcher.find()){
                // Remove extra space
                line = line.replaceAll(" +", " ");
                // Remove non-title ]] symbols.
                // More detail refer to https://github.com/SuprajaKalva/big_data_assessed_exercise/issues/1
                line = line.replaceAll("\\]\\]", "");

                // Remove stopwords
                List<String> allWords = new ArrayList<String>(Arrays.asList(line.toLowerCase().split(" ")));
                allWords.removeAll(stopWordList);
                line = String.join(" ", allWords);

                // Construct key-value pair.
                // Key: title of articles
                // Value: chunk of body
                context.write(new Text(temp_title), new Text(line));
            }else{
                // If it's title, simply set the current line to
                // temp_title.
                temp_title = line;
            }
        }
    }

    /**
     * Text Pre-processing Reducer
     * Concatenate all chunks belong to the same article.
     */
    public static class TPReducer extends Reducer<Text, Text, Text, Text>{

        public void reduce(Text temp_title, Iterable<Text> bodys, Context context)
                throws IOException, InterruptedException{
            String article_body = "";
            for(Text body: bodys){
                article_body += " " + body.toString();
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
    public static void tpRun(String [] args)
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

        // The input is a folder.
        MultipleInputs.addInputPath(job, new Path("src/main/resources/Mockdata"), TextInputFormat.class);
        //MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class); // changed it to args...
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws Exception the exception
     */
    public static void main(String[] args)
            throws Exception{
        tpRun(args);
    }
}
