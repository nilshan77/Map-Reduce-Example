package com.home;

import java.io.File;
import java.net.URI;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JoinMapTest extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		
		int exitStatus = ToolRunner.run(new JoinMapTest(), args);
		System.exit(exitStatus);
	}
	
	public int run(String[] args) throws Exception {
		
		Job job = Job.getInstance(getConf());
		job.setJobName("Join Map Example");
		job.setJarByClass(JoinMapTest.class);
		
		String[] paths = new GenericOptionsParser(args).getRemainingArgs();
		
		/** set File input and out */
		FileInputFormat.addInputPath(job, new Path(paths[0]));
		//1 job.setCacheFiles(new URI[] { new File(paths[1]).toURI()});
		DistributedCache.addCacheFile(new URI("/home/nilshan/input/joinmap/weather"), job.getConfiguration());
		FileOutputFormat.setOutputPath(job, new Path(paths[2]));
		
		/** set Map */
		job.setMapperClass(JoinMapper.class);
		job.setMapOutputKeyClass(FlightKey.class);
		job.setMapOutputValueClass(FlightValue.class);
		
		/** set sorting */
		//job.setGroupingComparatorClass(FlightGrouping.class);
		
		/** set reducer */
		job.setReducerClass(JoinReducer.class);
		
		/** set output */
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		int returnStatus = job.waitForCompletion(true) ? 0 : 1 ;
		
		System.out.println(job.getJobName()+" executed status :"+ job.isSuccessful());
		
		return returnStatus;
	}

	
	
}
