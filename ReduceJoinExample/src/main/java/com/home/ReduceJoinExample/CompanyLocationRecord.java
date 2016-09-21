package com.home.ReduceJoinExample;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class CompanyLocationRecord implements Writable {

	private IntWritable numBranches;
	private Text country;

	public CompanyLocationRecord() {
		country = new Text();
		numBranches = new IntWritable();
	}

	public void set(String cntry, int numBranches) {
		this.country.set(cntry);
		this.numBranches.set(numBranches);
	}

	public void write(DataOutput out) throws IOException {
		this.country.write(out);
		this.numBranches.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		this.country.readFields(in);
		this.numBranches.readFields(in);
	}

	public Text getCountry() {
		return country;
	}

	public IntWritable getNumBranches() {
		return numBranches;
	}
}
