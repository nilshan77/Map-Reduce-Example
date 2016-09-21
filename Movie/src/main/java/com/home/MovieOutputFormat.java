package com.home;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieOutputFormat<K, V> extends FileOutputFormat<K, V> {

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
		
		Path path = FileOutputFormat.getOutputPath(job);
		Path fullPath = new Path(path,"movie_result.xml");
		
		FileSystem fs = path.getFileSystem(job.getConfiguration());
		
		FSDataOutputStream fileOut = fs.create(fullPath, job);
		
		return new MovieRecordWriter<K, V>(fileOut);
	}

}