package com.home.ReduceJoinExample;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CompanyLocMapper extends Mapper<LongWritable, Text, CompanyIdKey, GenericCompWritable> {

	private CompanyIdKey compIdKey = new CompanyIdKey();

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, CompanyIdKey, GenericCompWritable>.Context context)
					throws IOException, InterruptedException {

		String[] recordFields = value.toString().split("\\t");

		compIdKey.set(Integer.parseInt(recordFields[0]), CompanyIdKey.COMPANYLOCRECORD);

		CompanyLocationRecord cmpLocRec = new CompanyLocationRecord();
		cmpLocRec.set(recordFields[1],Integer.parseInt(recordFields[2]));

		GenericCompWritable compWritable = new GenericCompWritable(cmpLocRec);
		context.write(compIdKey, compWritable);
	}
}
