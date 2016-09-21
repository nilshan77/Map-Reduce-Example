package com.home.Srjoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SRMapper extends Mapper<LongWritable, Text, Text, GenericSrWritable> {
	
	Text mapkey = new Text();
	ServiceRequest sr = new ServiceRequest();
	
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, GenericSrWritable>.Context context)
					throws IOException, InterruptedException {
		
		String[] fields = value.toString().split(",");
		
		mapkey.set(fields[0]+"-SR");
		sr.set(fields[1], fields[2]);
		GenericSrWritable gsrWritable = new GenericSrWritable();
		gsrWritable.set(sr);
		context.write(mapkey, gsrWritable);
	}

}
