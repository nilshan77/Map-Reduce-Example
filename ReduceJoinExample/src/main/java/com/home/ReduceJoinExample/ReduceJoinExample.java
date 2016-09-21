package com.home.ReduceJoinExample;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Hello world!
 *
 */
public class ReduceJoinExample extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int exitStatus = ToolRunner.run(new ReduceJoinExample(), args);
		System.exit(exitStatus);
	}

	public int run(String[] args) throws Exception {

		Job job = Job.getInstance(getConf());
		job.setJobName("Reduce Join Example");
		job.setJarByClass(ReduceJoinExample.class);

		String[] paths = new GenericOptionsParser(args).getRemainingArgs();
		
		/* Set Input / Output file path */
		MultipleInputs.addInputPath(job, new Path(paths[0]), TextInputFormat.class, CompanyMapper.class);
		MultipleInputs.addInputPath(job, new Path(paths[1]), TextInputFormat.class, CompanyLocMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(paths[2]));
		
		/* Set Map and Map output setting */
		job.setMapOutputKeyClass(CompanyIdKey.class);
		job.setMapOutputValueClass(GenericCompWritable.class);
		
		/* Set partition / sort / grouping */
		job.setSortComparatorClass(CompanySorting.class);
		job.setGroupingComparatorClass(CompanyGrouping.class);
		
		/* Set reducer */
		job.setReducerClass(CompanyReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		/* Set input output file format */
		//job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputFormatClass(MyFileOutput.class);
		
		int returnStatus = job.waitForCompletion(true) ? 0 : 1;
		System.out.println(job.getJobName() + " is executed successfully :" + job.isSuccessful());

		return returnStatus;

	}

}
