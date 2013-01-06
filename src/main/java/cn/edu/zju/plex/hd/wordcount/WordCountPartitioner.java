package cn.edu.zju.plex.hd.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountPartitioner extends Partitioner<Text, IntWritable>{

	public static int getPartitionNumbers() {
		return 3;
	}
	
	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) {
		String str = key.toString();

		if (str.startsWith("i") || str.startsWith("w")) {
			return 1;
		}
		else if (str.startsWith("t")) {
			return 2;
		}
		
		return 0;
	}
	
}
