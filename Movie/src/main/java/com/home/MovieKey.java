package com.home;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class MovieKey implements WritableComparable<MovieKey>{

	private IntWritable year;
	private IntWritable grossEarning;
	private Text genre;
	
	public MovieKey() {
		year = new IntWritable();
		grossEarning = new IntWritable();
		genre = new Text();
	}
	
	public MovieKey(IntWritable year, IntWritable ge, Text gnre) 
	{
		this.year = year;
		this.grossEarning = ge;
		this.genre = gnre;
	}
	
	public void set(IntWritable year, IntWritable ge, Text gnre) 
	{
		this.year = year;
		this.grossEarning = ge;
		this.genre = gnre;
	}
	
	public void write(DataOutput out) throws IOException {
		year.write(out);
		grossEarning.write(out);
		genre.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		this.year.readFields(in);
		this.grossEarning.readFields(in);
		this.genre.readFields(in);
	}

	public int compareTo(MovieKey nextMovie) {
		//int res = this.name.toString().compareTo(nextMovie.getName().toString());
		int res = this.year.compareTo(nextMovie.year);
		
		if(res == 0) 
			res = this.genre.compareTo(nextMovie.genre);
				
		return res;
	}
	
	public IntWritable getYear() {
		return year;
	}

	public void setYear(IntWritable year) {
		this.year = year;
	}

	public IntWritable getGrossEarning() {
		return grossEarning;
	}

	public void setGrossEarning(IntWritable grossEarning) {
		this.grossEarning = grossEarning;
	}

	public Text getGenre() {
		return genre;
	}

	public void setGenre(Text genre) {
		this.genre = genre;
	}

	@Override
	public String toString() {
		
		StringBuffer sb = new StringBuffer();
		sb.append("year :"+getYear());
		sb.append(", Earning :"+getGrossEarning());
		sb.append(", Genre :"+getGenre());
		
		return sb.toString();
	}
}
