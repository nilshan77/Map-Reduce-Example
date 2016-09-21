package com.home.ReduceJoinExample;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class CompanyRecord implements Writable {

	private Text name;

	public CompanyRecord() {
		name = new Text();
	}

	public void set(String name) {
		this.name.set(name);
	}

	public void write(DataOutput out) throws IOException {
		this.name.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		this.name.readFields(in);
	}

	public Text getName() {
		return name;
	}
}
