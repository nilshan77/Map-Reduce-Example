package com.home;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class MovieValue implements WritableComparable<MovieValue>{

	private Text name;
	private IntWritable year;
	private IntWritable grossEarning;
	private FloatWritable ratings;
	private Text genre;
	
	public MovieValue() {
		name = new Text();
		year = new IntWritable();
		grossEarning = new IntWritable();
		ratings = new FloatWritable();
		genre = new Text();
	}
	
	public MovieValue(Text name, IntWritable year, IntWritable ge, FloatWritable rt, Text gnre) 
	{
		this.name = name;
		this.year = year;
		this.grossEarning = ge;
		this.ratings = rt;
		this.genre = gnre;
	}
	
	public void set(Text name, IntWritable year, IntWritable ge, FloatWritable rt, Text gnre) 
	{
		this.name = name;
		this.year = year;
		this.grossEarning = ge;
		this.ratings = rt;
		this.genre = gnre;
	}
	
	public void write(DataOutput out) throws IOException {
		name.write(out);
		year.write(out);
		grossEarning.write(out);
		ratings.write(out);
		genre.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		this.name.readFields(in);
		this.year.readFields(in);
		this.grossEarning.readFields(in);
		this.ratings.readFields(in);
		this.genre.readFields(in);
	}

	public int compareTo(MovieValue nextMovie) {
		//int res = this.name.toString().compareTo(nextMovie.getName().toString());
		int res = this.year.compareTo(nextMovie.year);
		
		if(res == 0) 
			res = this.genre.compareTo(nextMovie.genre);
				
		return res;
	}
	
	public Text getName() {
		return name;
	}

	public void setName(Text name) {
		this.name = name;
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

	public FloatWritable getRatings() {
		return ratings;
	}

	public void setRatings(FloatWritable ratings) {
		this.ratings = ratings;
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
		sb.append("Name :"+getName());
		sb.append(", year :"+getYear());
		sb.append(", Earning :"+getGrossEarning());
		sb.append(", Rating :"+getRatings());
		sb.append(", Genre :"+getGenre());
		
		return sb.toString();
	}

	
}
