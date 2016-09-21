package com.home.ReduceJoinExample;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class CompanyReducer extends Reducer<CompanyIdKey, GenericCompWritable, NullWritable, Text> {

	@Override
	protected void reduce(CompanyIdKey key, Iterable<GenericCompWritable> values,
			Reducer<CompanyIdKey, GenericCompWritable, NullWritable, Text>.Context context)
					throws IOException, InterruptedException {

		StringBuffer sb = new StringBuffer();
		int locCnt = 0;

		for (GenericCompWritable comp : values) {

			Writable instance = comp.get();

			if (key.getKeyType().toString().equals(CompanyIdKey.COMPANYINFORECORD)) {
				CompanyRecord record = (CompanyRecord) instance;
				sb.append("company " + record.getName()+" has ");

			} else {
				CompanyLocationRecord record = (CompanyLocationRecord) instance;
				locCnt += record.getNumBranches().get();
			}
		}
		context.write(NullWritable.get(), new Text(sb.toString()+locCnt+" branches wordwide"));
	}
}
