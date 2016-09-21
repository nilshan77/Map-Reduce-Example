package com.home;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieMapper extends Mapper<LongWritable, Text, MovieKey, MovieValue> 
{
	MovieKey movieKey = new MovieKey();
	MovieValue movieValue = new MovieValue();
	private Text name = new Text();
	private IntWritable year = new IntWritable();
	private IntWritable grossEarning = new IntWritable();
	private FloatWritable ratings = new FloatWritable();
	private Text genre = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
	
		System.out.println("In a Map");
		
		if(value.toString() != null && !value.toString().isEmpty()){
			
			String[] split = value.toString().split("\t");
			
			name.set(split[0]);
			year.set(Integer.parseInt(split[1]));
			grossEarning.set(Integer.parseInt(split[2]));
			ratings.set(Float.parseFloat(split[3]));
			genre.set(split[4]);
			
			movieValue.set(name, year, grossEarning , ratings , genre);
			movieKey.set(year, grossEarning, genre);
			context.write(movieKey, movieValue);
		}
	}
}
