package com.home;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class MovieExample extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		
		int exitCode = ToolRunner.run(new MovieExample(), args);
		System.exit(exitCode);		
	}

	public int run(String[] args) throws Exception {
				 
		Job job = Job.getInstance();
		job.setJarByClass(MovieExample.class);
		job.setJobName("Movie Example");
		
		/** Set file path to be processed */
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		/** Set Mapper */
		job.setMapperClass(MovieMapper.class);
		job.setMapOutputKeyClass(MovieKey.class);
		job.setMapOutputValueClass(MovieValue.class);
		
		/** Set Partitioner */
		job.setPartitionerClass(MoviePartitioner.class);
		job.setNumReduceTasks(4);
		
		/** Set Grouping  / Sorting */
		job.setSortComparatorClass(MovieSorting.class);
		job.setGroupingComparatorClass(MovieGroupComparator.class);
		
		/** Set Reducer */
		job.setReducerClass(MovieReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		/** Set output format */
		job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(MovieOutputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		int returnCode = job.waitForCompletion(true) ? 0 : 1;
				
		System.out.println("----------------- status :"+job.isSuccessful());	
		
		return returnCode;
	}
}
