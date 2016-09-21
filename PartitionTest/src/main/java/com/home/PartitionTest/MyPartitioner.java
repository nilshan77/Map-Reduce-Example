package com.home.PartitionTest;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, Text> {
	@Override
	public int getPartition(Text key, Text value, int numPartitions) {

		return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
	}
}
