package MapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
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
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Data Preprocess Class
 *
 * <p>Output file: 1. Term occurrence 2. Documents' length 3. Term IDF 4. Pre-compute coefficient
 *
 * @author Molin Liu
 */
public class DataPreprocess {

  /** Text Preprocessing Mapper */
  public static final Pattern HEAD_PATTERN = Pattern.compile("^\\[{2}.*\\]{2}");
  /** The constant num_doc. */
  public static int num_doc = 0;

  /** The constant total_docLen. */
  public static int total_docLen = 0;

  /** The constant avgLen. */
  public static float avgLen = (float) 0.0;
  /** Text Pre-processing Mapper Class Input type: gzip files */
  public static class TPMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static String temp_title;
        //private static List<String> stopWordList;

        protected void setup(Context context) throws IOException, InterruptedException {
            /**
            try {
                // BufferedReader fis = new BufferedReader(new FileReader(stopWordFile_PATH));
                //this.stopWordList = Files.readAllLines(Paths.get("src/main/resources/stopword-list.txt"));
            } catch (IOException ioe) {
                System.err.println("Exception while reading stop word file" + ioe.toString());
            }
             */
        }

        public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
            String line = lineText.toString();
            // Preprocess the input text file.
            line = line.trim();
            // Remove subtitle
            line = line.replaceAll("={2}.*={2}", "");
            // Remove extra space
            line = line.replaceAll(" +", " ");
            Matcher titleMatcher = HEAD_PATTERN.matcher(line);
            if (line.equals("")) {
                return;
            }
            // See if the current line is title:
            if (!titleMatcher.find()) {
                // Remove non-ASCII characters
                line = line.replaceAll("[^A-Za-z0-9\\[\\]]", " ");
                // Remove unbound ]] symbols.
                // More detail refer to
                // https://github.com/SuprajaKalva/big_data_assessed_exercise/issues/1
                line = line.replaceAll("\\]\\]", "");

                // Remove stopwords
                List<String> allWords = new ArrayList<String>(Arrays.asList(line.toLowerCase().split(" ")));
                //allWords.removeAll(stopWordList);
                line = String.join(" ", allWords);

                //Porter Stemmer
                List<String> allWordsToStem = new ArrayList<String>(Arrays.asList(line.toLowerCase().split(" ")));
                MapReduce.PorterStemmer porterStemmer = new MapReduce.PorterStemmer();
                List<String> ported_words = new ArrayList<>();
                allWordsToStem.forEach(word -> {
                    ported_words.add(porterStemmer.stem(word));
                });
                line = String.join(" ", ported_words);

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

  /** Text Pre-processing Reducer Concatenate all chunks belong to the same article. */
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
            //num_doc += 1;
            context.write(temp_title, new Text(article_body));
        }
    }

  /** The type Doc coef mapper. */
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
                context.write(new Text(key_title), new IntWritable(docLen));
            }
        }
    }

  /** The type Doc coef reducer. */
  public static class DocCoefReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {
        private float avgLen;
        private MultipleOutputs<Text, FloatWritable> mos;
        public void setup(Context context)
                throws IOException, InterruptedException{
            mos = new MultipleOutputs<>(context);
            Configuration conf = context.getConfiguration();
            this.avgLen = Float.parseFloat(conf.get("average").trim());
            //avgLen = (float)total_docLen/(float)num_doc;
            mos.write("Avg", new Text("Avg"), new FloatWritable(this.avgLen));
            mos.close();
            System.out.println("The average length is " + avgLen);
        }
        public void reduce(Text title, Iterable<IntWritable> docLens, Context context)
                throws IOException, InterruptedException{
            float docCoe = (float) 0.375;
            for (IntWritable docLen : docLens){
                docCoe+= 1.125 * docLen.get() / avgLen;
            }
            context.write(new Text(title), new FloatWritable(docCoe));
        }
    }

  /** The type Doc tf mapper. Output the Documents' length */
  public static class DocTFMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text word = new Text();
        private final static IntWritable one = new IntWritable(1);
        private MultipleOutputs<Text, IntWritable> mos;

        public void setup(Context context){
            mos = new MultipleOutputs<>(context);
        }

        public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
            String line = lineText.toString();
            line = line.trim();
            if (line.equals("")){
                return;
            }
            Matcher head_matcher = HEAD_PATTERN.matcher(line);
            if (head_matcher.find()) {
                String key_title = head_matcher.group(0);
                int temp_docLen = line.split("\\s+").length;
                mos.write("DocLen", new Text(key_title), new IntWritable(temp_docLen));
                StringTokenizer itr = new StringTokenizer(line);
                while (itr.hasMoreTokens()) {
                    word.set(itr.nextToken() + "-" + key_title);
                    context.write(word, one);
                }
                //total_docLen+=docLen;
            }
        }
        public void cleanup(Context context)
                throws IOException, InterruptedException {
            mos.close();
        }
    }

  /** The type Doc tf reducer. The output file is term occurrences in each document. */
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

  /** The type Idf mapper. */
  public static class IDFMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
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
   *
   * <p>Output the IDF
   */
  public static class IDFReducer extends Reducer<Text, Text, Text, FloatWritable> {
        private int num_doc;
        public void setup(Context context){
            Configuration conf = context.getConfiguration();
            this.num_doc = (int)Float.parseFloat(conf.get("num_doc").trim());
        }

        public void reduce(Text word, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> valCache = new ArrayList<>();
            for (Text value : values) {
                valCache.add(value.toString());
            }
            float IDF = (float) Math.log10((this.num_doc-valCache.size()+0.5)/(valCache.size()+0.5));
            for (String s : valCache) {
                String[] article_tf = s.toString().split("=");
                context.write(new Text(word + "-" + article_tf[0]), new FloatWritable(IDF));
            }

        }
    }

  /**
   * Average len float [ ].
   *
   * This method is used to read the file in HDFS
   * However it still not supports AWS s3 path.
   * @param doclen_dir the doclen dir
   * @return the float [ ]
   * @throws IOException the io exception
   */
  public static float[] averageLen(Path doclen_dir) throws IOException {
        float average;
        int num_doc = 0;
        int total_len = 0;
        Configuration conf = new Configuration();
        try{
            FileSystem fs = FileSystem.get(conf);
            // Read all files start with `DocLen`, which store the length of documents.
            FileStatus[] fileStatus = fs.listStatus(doclen_dir, new PathFilter(){
                @Override
                public boolean accept(Path path) {
                    return path.getName().startsWith("DocLen");
                }
            });

            for(FileStatus status : fileStatus){
                FSDataInputStream fdsis = fs.open(status.getPath());
                BufferedReader br = new BufferedReader(new InputStreamReader(fdsis));
                String line = "";
                while ((line = br.readLine()) != null) {
                    num_doc+=1;
                    line = line.trim();
                    total_len+=Integer.parseInt(line.split("\t")[1]);
                }
                br.close();
                System.out.println(status.getPath().toString());
            }
        }catch (Exception e){
            System.out.println("Error at averageLen");
            e.printStackTrace();
        }
        average = (float)total_len/num_doc;
      return new float[]{average, (float)num_doc};
    }
  /**
   * Averge len test.
   *
   * @throws IOException the io exception
   */
  @Test
  void avergeLenTest() throws IOException {
        System.out.println(Arrays.toString(averageLen(new Path("/Users/meow/Documents/Projects/UoG_S2/BD/Report/Data/output/output_qt/data/2tf"))));
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
        /*
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

        /*
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

        /*
         * Calculate the average length of documents
         */
        float[] result = averageLen(new Path(s2_outdir));
        float average_len = result[0];
        Configuration s4_conf = new Configuration();
        s4_conf.set("average", String.valueOf(average_len));
        System.out.println("The average length of document is "+ average_len);

        float num_doc = result[1];
        Configuration s3_conf = new Configuration();
        s3_conf.set("num_doc", String.valueOf(num_doc));
        System.out.println("The number of documents is "+ num_doc);
        /*
         * Stage 3: TF-IDF
         */
        String s3_outdir = output_dir + "/3idf";
        Job job3 = Job.getInstance(s3_conf, "IDF");
        job3.setJarByClass(DataPreprocess.class);
        job3.setMapperClass(DataPreprocess.IDFMapper.class);
        job3.setReducerClass(DataPreprocess.IDFReducer.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job3, new Path(s2_outdir+"/part*"));
        FileOutputFormat.setOutputPath(job3, new Path(s3_outdir));
        job3.waitForCompletion(true);

        String s4_outdir = output_dir+"/4coef";
        Job job4 = Job.getInstance(s4_conf, "Coefficient");
        job4.setJarByClass(DataPreprocess.class);
        job4.setMapperClass(DataPreprocess.DocCoefMapper.class);
        job4.setReducerClass(DataPreprocess.DocCoefReducer.class);
        job4.setMapOutputValueClass(IntWritable.class);

        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(FloatWritable.class);
        MultipleInputs.addInputPath(job4, new Path(s1_outdir+"/part*"), TextInputFormat.class);
        MultipleOutputs.addNamedOutput(job4, "Avg", TextOutputFormat.class, Text.class, FloatWritable.class);
        FileOutputFormat.setOutputPath(job4, new Path(s4_outdir));

        job4.waitForCompletion(true);
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
