package com.home;

import java.io.File;
import java.net.URI;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class MapJoinExample extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MapJoinExample(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception {
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(MapJoinExample.class);
		job.setJobName("MapJoin Example");
		
		/** Set File Input and Output Path */
		FileInputFormat.addInputPath(job, new Path(args[0]));
		URI[] uris = {new File(args[1]).toURI()};
		job.setCacheFiles(uris);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		/** Mapper */
		job.setMapperClass(MapJoinMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		/** Partition */
		
		/** Sorting / Grouping */
		
		/** Reducer */
		job.setNumReduceTasks(0);
		
		/** File input / output format */
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		int returnStatus = job.waitForCompletion(true) ? 0 : 1;
		
		System.out.println("Job is Successful :"+ job.isSuccessful());
		
		return returnStatus;
	}
	
}
