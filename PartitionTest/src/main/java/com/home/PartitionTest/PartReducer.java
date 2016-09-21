package com.home.PartitionTest;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PartReducer extends Reducer<Text, Text, NullWritable, Text> {
	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1, Reducer<Text, Text, NullWritable, Text>.Context arg2)
			throws IOException, InterruptedException {

		StringBuffer sb = new StringBuffer();

		sb.append(arg0 + " with total:");

		Iterator<Text> itr = arg1.iterator();
		float sum = 0;
		while (itr.hasNext()) {
			Text t = itr.next();
			String fields[] = t.toString().split(",");
			float val = Float.parseFloat(fields[6].trim());
			sum += val;
		}
		sb.append(sum);
		arg2.write(NullWritable.get(), new Text(sb.toString()));
	}
}
