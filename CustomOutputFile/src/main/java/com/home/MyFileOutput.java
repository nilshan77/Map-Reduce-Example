package com.home;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyFileOutput<K,V> extends FileOutputFormat<K, V>
{

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
		
		Path outputPath = FileOutputFormat.getOutputPath(job);
		FileSystem fs = outputPath.getFileSystem(job.getConfiguration());
		FSDataOutputStream fsOut = fs.create(new Path(outputPath,"myfile.txt") , true);
		return new MyRecordWriter<K,V>(fsOut);
	}
	
	public static class MyRecordWriter<K,V> extends RecordWriter<K, V>
	{
		DataOutputStream out;
		private static final String utf8 = "UTF-8";
		
		public MyRecordWriter(DataOutputStream fsOut) {
			this.out = fsOut;
		}
		
		/**
		 * Write the object to the byte stream, handling Text as a special case.
		 *
		 * @param o
		 *            the object to print
		 * @throws IOException
		 *             if the write throws, we pass it on
		 */
		private void writeObject(Object o) throws IOException {
			if (o instanceof Text) {
				Text to = (Text) o;
				out.write(to.getBytes(), 0, to.getLength());
			} else {
				out.write(o.toString().getBytes(utf8));
			}
		}
		
		@Override
		public void write(K key, V value) throws IOException, InterruptedException {
			boolean nullKey = key == null || key instanceof NullWritable;
			boolean nullValue = value == null || value instanceof NullWritable;

			if (nullKey && nullValue) {
				return;
			}
			
			writeObject(key);
			this.out.writeUTF("\n");
			writeObject(value);
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			this.out.close();
		}
		
	}
	
}
