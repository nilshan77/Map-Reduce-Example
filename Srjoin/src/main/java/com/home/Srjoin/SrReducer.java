package com.home.Srjoin;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class SrReducer extends Reducer<Text, GenericSrWritable, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<GenericSrWritable> values,
			Reducer<Text, GenericSrWritable, Text, Text>.Context context) throws IOException, InterruptedException {

		StringBuffer sb = new StringBuffer();

		String[] k = key.toString().split("-");

		for (GenericSrWritable gsrWritable : values) {
			Writable record = gsrWritable.get();

			if (record.getClass().isAssignableFrom(ServiceDetails.class)) {

				ServiceDetails details = (ServiceDetails) record;
				sb.append("\n<"+details.getContactName() + ">\n<" + details.getPhNum() + ">\n<" + details.getCompanyName()+">");

			} else {

				ServiceRequest req = (ServiceRequest) record;
				sb.append("\n<"+req.getStatus() + ">" + req.getDate() + "</"+req.getStatus() + ">");
			}
		}

		context.write(new Text(k[0]), new Text(sb.toString()));

	}
}
