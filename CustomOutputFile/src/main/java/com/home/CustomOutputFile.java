package com.home;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CustomOutputFile extends Configured implements Tool {

	public static void main(String... args) throws Exception {

		int exitStatus = ToolRunner.run(new CustomOutputFile(), args);
		System.exit(exitStatus);

	}

	public int run(String[] args) throws Exception {

		Job job = Job.getInstance(getConf());
		job.setJobName("Custom output file example");
		job.setJarByClass(CustomOutputFile.class);

		String[] paths = new GenericOptionsParser(args).getRemainingArgs();
		
		/* File input / output pat */
		FileInputFormat.addInputPath(job, new Path(paths[0]));
		FileOutputFormat.setOutputPath(job, new Path(paths[1]));
		
		/* Map */
		
		/* sort / group / partition */
		
		/* Reduce */
		
		/* File input and output format */
		
		int returnStatus = job.waitForCompletion(true) ? 0 : 1;
		System.out.println(job.getJobName() + " executed successfully :" + job.isSuccessful());

		return returnStatus;
	}

}
