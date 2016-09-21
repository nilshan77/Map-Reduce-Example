package com.home.Srjoin;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * Hello world!
 *
 */
public class App extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int exitStatus = ToolRunner.run(new App(), args);
		System.exit(exitStatus);
	}

	public int run(String[] args) throws Exception {

		Job job = Job.getInstance(getConf());
		job.setJarByClass(App.class);
		job.setJobName("Service Request Reduce Example");

		String[] paths = new GenericOptionsParser(args).getRemainingArgs();

		/* Set input and output paths */
		MultipleInputs.addInputPath(job, new Path(paths[0]), TextInputFormat.class, SRMapper.class);
		MultipleInputs.addInputPath(job, new Path(paths[1]), TextInputFormat.class, SRDetailsMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(paths[2]));

		/* Set map and map output */
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(GenericSrWritable.class);

		/* Set sorting */
		job.setGroupingComparatorClass(SrGroupComparator.class);

		/* Set Reducer */
		job.setReducerClass(SrReducer.class);

		/* Set input and output format */ 
		job.setOutputFormatClass(ServiceOutputFormat.class);

		int returnStatus = job.waitForCompletion(true) ? 0 : 1;
		System.out.println(job.getJobName() + "is successfully Completed :" + job.isSuccessful());
		return returnStatus;
	}
}
