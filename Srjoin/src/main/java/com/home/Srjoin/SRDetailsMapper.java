package com.home.Srjoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SRDetailsMapper extends Mapper<LongWritable, Text, Text, GenericSrWritable> {

	Text mapkey = new Text();
	ServiceDetails srdetails = new ServiceDetails();
	

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, GenericSrWritable>.Context context)
					throws IOException, InterruptedException {
		
		String[] fields = value.toString().split(",");

		mapkey.set(fields[0]+"-SRDetails");
		srdetails.set(fields[1], fields[3], fields[2]);
		GenericSrWritable gsrWritable = new GenericSrWritable();
		gsrWritable.set(srdetails);
		context.write(mapkey, gsrWritable);
	}
}
