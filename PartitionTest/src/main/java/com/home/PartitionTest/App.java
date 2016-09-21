package com.home.PartitionTest;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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
		job.setJobName("Partion Test");

		String[] paths = new GenericOptionsParser(args).getRemainingArgs();

		/** set input and output path */
		FileInputFormat.addInputPath(job, new Path(paths[0]));
		FileOutputFormat.setOutputPath(job, new Path(paths[1]));

		/** Set Map and map output */
		job.setMapperClass(PartMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		/** Set sort / group / */
		job.setGroupingComparatorClass(PartGroup.class);
		job.setPartitionerClass(MyPartitioner.class);

		/** Set Reducer */
		//job.setCombinerClass(PartReducer.class);
		job.setReducerClass(PartReducer.class);
		job.setNumReduceTasks(2);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		/** Set Input and output format */
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		int returnStatus = job.waitForCompletion(true) ? 0 : 1;
		System.out.println(job.getJobName() + "executed successfully :" + job.isSuccessful());

		return returnStatus;
	}
}
