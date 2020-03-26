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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class DocProcessMapReduce {

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
     * The type Document Term Frequency Mapper.
     *
     * @version 1.2.0
     * Add MultipleOutputs feature
     * It will generate a text file with document length
     */
    public static class DocTFMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text word = new Text();
        private final static IntWritable one = new IntWritable(1);
        private MultipleOutputs<Text, IntWritable> mos;

        protected void setup(Context context){
            mos = new MultipleOutputs<>(context);
        }
        public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
            String line = lineText.toString();
            Matcher head_matcher = HEAD_PATTERN.matcher(line);
            if (head_matcher.find()) {
                String key_title = head_matcher.group(0);
                StringTokenizer itr = new StringTokenizer(line);
                int doc_len = 0;
                while (itr.hasMoreTokens()) {
                    word.set(itr.nextToken() + "-" + key_title);
                    doc_len+=1;
                    context.write(word, one);
                }
                mos.write("DocLen", new Text(key_title), new IntWritable(doc_len));
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
     * The type Idf mapper.
     */
    public static class IDFMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
            String line = lineText.toString();
            String term_title = line.split("\t")[0];
            String termFreq = line.split("\t")[1];
            String term = term_title.split("-")[0];
            String articleTitle = term_title.split("-")[1];
            String term_title_tf = articleTitle + "=" + termFreq;
            context.write(new Text(term), new Text(term_title_tf));
        }
    }

    /**
     * The type Idf reducer.
     */
    public static class IDFReducer extends Reducer<Text, Text, Text, FloatWritable> {
        public void reduce(Text word, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> valCache = new ArrayList<>();

            for (Text value : values) {
                valCache.add(value.toString());
            }
            float IDF;
            float TFIDF;

            for (int i = 0; i < valCache.size(); i++) {
                IDF = (float) Math.log10(1 + (num_doc / valCache.size()));
                String[] article_tf = (valCache.get(i)).toString().split("=");

                TFIDF = IDF * Float.parseFloat(article_tf[1].trim());
                context.write(new Text(word + "-" + article_tf[0]), new FloatWritable(TFIDF));
            }
        }
    }
}
