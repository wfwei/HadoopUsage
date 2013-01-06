package cn.edu.zju.plex.hd.wordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {
	private static final String NAME = "WordCount";

	private static void usage() {
		System.err.println("usage: " + NAME
				+ " input output [-c] [-p]\n\t-c Use Combiner class. "
				+ "\n\t-p Use self-defined partitioner.");
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		if (args.length < 2) {
			usage();
			return;
		}

		Configuration conf = new Configuration();

		Job job = new Job(conf, NAME);
		job.setJarByClass(WordCountMapper.class);
		job.setJarByClass(WordCountReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		List<String> otherArgs = new ArrayList<String>();
		for (int i = 2; i < args.length; ++i ) {
			otherArgs.add(args[i]);
		}
		if (otherArgs.contains("-c")) {
			job.setCombinerClass(WordCountReducer.class);
		}

		if (otherArgs.contains("-p")) {
			job.setNumReduceTasks(WordCountPartitioner.getPartitionNumbers());
			job.setPartitionerClass(WordCountPartitioner.class);
			job.setJarByClass(WordCountPartitioner.class);
		}

		job.waitForCompletion(true);
	}
}
