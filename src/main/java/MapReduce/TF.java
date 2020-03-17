package com.neword;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;

public class TF {

	/*
	 * public static class TF_Mapper extends Mapper<LongWritable,Text,Text,Text>{
	 * private Text word_key = new Text(); private Text termOcc = new Text();
	 * 
	 * public void map(LongWritable key, Text values, Context context) throws
	 * IOException,InterruptedException{ //String[] word_f =
	 * values.toString().split("\t"); // this.word_key.set(word_f[0]); //
	 * this.filename_tf.set(word_f[0] + "=" + word_f[1]);
	 * this.word_key.set(key.toString()); this.termOcc.set(values);
	 * context.write(word_key,termOcc); } }
	 * 
	 * public static class TF_Reducer extends
	 * Reducer<Text,Text,Text,DoubleWritable>{ private Text word_key = new Text();
	 * private double tf;
	 * 
	 * public void reduce(Text key, Iterable<Text> values, Context context) throws
	 * IOException, InterruptedException { String a= values.toString(); double val =
	 * Double.valueOf(a); tf = Math.log10(10) + Math.log10(val);
	 * context.write(this.word_key, new DoubleWritable(this.tf)); }
	 * 
	 * }
	 */

	public static class TF_Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);

		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			String line = lineText.toString();
			// String fileName = ((FileSplit) context.getInputSplit()).getPath()
			// .getName();

			Text currentWord = new Text();
			for (String word : WORD_BOUNDARY.split(line)) {
				if (word.isEmpty()) {
					continue;
				}

				currentWord = new Text(word);
				context.write(currentWord, one);
			}
		}
	}

	public static class TF_Reduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {

		public void reduce(Text word, Iterable<IntWritable> counts, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : counts) {
				sum += count.get();
			}
			// Calculate Term Frequency in logarithmic form
			double tf = Math.log10(10) + Math.log10(sum);
			// Change the return type to DoubleWritable to return double value
			context.write(word, new DoubleWritable(tf));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(TF.class);

		job.setMapperClass(TF_Map.class);
		// job.setCombinerClass(TF_Reducer.class);
		job.setReducerClass(TF_Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
