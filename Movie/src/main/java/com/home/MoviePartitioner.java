package com.home;

import org.apache.hadoop.mapreduce.Partitioner;

public class MoviePartitioner extends Partitioner<MovieKey, MovieValue> {

	@Override
	public int getPartition(MovieKey key, MovieValue value, int numPartitions) {
		System.out.println("In a Partition: "+key.toString());
		
		if(key.getYear().get() == 2012)
			return 0;
		
		if(key.getYear().get() == 2013)
			return 1;
		
		if(key.getYear().get() == 2014)
			return 2;
		
		if(key.getYear().get() == 2015)
			return 3;
		
		return 0;
	}
	
}
