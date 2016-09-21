package com.home.Srjoin;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ServiceOutputFormat<K,V> extends FileOutputFormat<K, V>
{
	public ServiceOutputFormat() {
	}

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {

		Path path = FileOutputFormat.getOutputPath(job);
		FileSystem fs = path.getFileSystem(job.getConfiguration());
		FSDataOutputStream out = fs.create(new Path(path, "ServiceRequest.xml"), true);
		return new ServiceOutputWriter(out);
	}
	
	class ServiceOutputWriter extends RecordWriter<K, V>
	{
		DataOutputStream out;
		
		public ServiceOutputWriter(DataOutputStream out) throws IOException {
			this.out = out;
			this.out.writeBytes("<xml>");
		}
		
		@Override
		public void write(K key, V value) throws IOException, InterruptedException {
			
			String srnum = "\n<"+key.toString()+">";
			out.writeBytes(srnum);
			
			out.writeBytes(value.toString());
			
			srnum = "\n</"+key+">";
			out.writeBytes(srnum);
			
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			try {
				out.writeBytes("\n</xml>");
			} finally {
				// even if writeBytes() fails, make sure we close the stream
				out.close();
			}
		}
		
	}
}
