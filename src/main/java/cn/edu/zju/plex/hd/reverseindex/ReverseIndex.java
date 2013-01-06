package cn.edu.zju.plex.hd.reverseindex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
/**
 * ref: http://developer.yahoo.com/hadoop/tutorial/module4.html#prereq
 * TODO: http://www.cnblogs.com/BigBesom/archive/2012/04/14/2446775.html
 * 
 * @author plex
 *
 */
public class ReverseIndex {
	private static final String NAME = "ReverseIndex";

	private static void usage() {
		System.err.println("usage: " + NAME
				+ " input output [-c]\n\t-c Use Combiner class. ");
	}

	public static class ReverseIndexMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		private Text word = new Text();

		@Override
		public void map(LongWritable offset, Text value, Context context)
				throws IOException, InterruptedException {

			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			Text doc = new Text(fileSplit.getPath().getName());

			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, doc);
			}
		}
	}

	public static class ReverseIndexReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			StringBuffer strbuf = new StringBuffer();
			for (Text val : values) {
				strbuf.append(val.toString() + "\t");
			}
			context.write(key, new Text(strbuf.toString()));
		}
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		if (args.length < 2) {
			usage();
			return;
		}

		Configuration conf = new Configuration();

		Job job = new Job(conf, NAME);
		job.setJarByClass(ReverseIndex.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(ReverseIndexMapper.class);
		job.setReducerClass(ReverseIndexReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		List<String> otherArgs = new ArrayList<String>();
		for (int i = 2; i < args.length; ++i) {
			otherArgs.add(args[i]);
		}
		if (otherArgs.contains("-c")) {
			job.setCombinerClass(ReverseIndexReducer.class);
		}

		job.waitForCompletion(true);
	}
}
