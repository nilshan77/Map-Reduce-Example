package com.home;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class FlightKey implements WritableComparable<FlightKey> {

	private IntWritable year;
	private IntWritable month;
	private IntWritable day;
	private IntWritable arrDelay;

	public FlightKey() {
		year = new IntWritable();
		month = new IntWritable();
		day = new IntWritable();
		arrDelay = new IntWritable();
	}

	public void set(String yr, String mnt, String d, String arrD) {
		this.year.set(Integer.parseInt(yr));
		this.month.set(Integer.parseInt(mnt));
		this.day.set(Integer.parseInt(d));
		this.arrDelay.set(Integer.parseInt(arrD));
	}

	public int compareTo(FlightKey nextFlight) {

		Date d1 = this.getDate(this.year.get(), this.month.get(), this.day.get());
		Date d2 = nextFlight.getDate(nextFlight.getYear().get(), nextFlight.getMonth().get(),
				nextFlight.getDay().get());

		System.out.println("date 1 :"+d1);
		System.out.println("date 2 :"+d2);
		
		if (d1.compareTo(d2) == 0) {
			
			System.out.println("delay:"+this.getArrDelay()+" next flight"+nextFlight.getArrDelay());
			
			return -1 * this.getArrDelay().compareTo(nextFlight.getArrDelay());
		}
		return d1.compareTo(d2);
	}

	public void write(DataOutput out) throws IOException {
		year.write(out);
		month.write(out);
		day.write(out);
		arrDelay.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		year.readFields(in);
		month.readFields(in);
		day.readFields(in);
		arrDelay.readFields(in);
	}

	public IntWritable getYear() {
		return year;
	}

	public void setYear(IntWritable year) {
		this.year = year;
	}

	public IntWritable getMonth() {
		return month;
	}

	public void setMonth(IntWritable month) {
		this.month = month;
	}

	public IntWritable getDay() {
		return day;
	}

	public void setDay(IntWritable day) {
		this.day = day;
	}

	public IntWritable getArrDelay() {
		return arrDelay;
	}

	public void setArrDelay(IntWritable arrDelay) {
		this.arrDelay = arrDelay;
	}

	public Date getDate(int year, int month, int day) {
		Calendar cal = Calendar.getInstance();
		cal.set(year, month - 1, day, 0, 0);
		return cal.getTime();
	}

}
