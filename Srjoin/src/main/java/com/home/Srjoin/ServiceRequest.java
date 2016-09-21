package com.home.Srjoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class ServiceRequest implements Writable {

	private Text status, date;

	public ServiceRequest() {
		status = new Text();
		date = new Text();
	}

	public void set(String stauts, String date) {
		this.status.set(stauts);
		this.date.set(date);
	}

	public void write(DataOutput out) throws IOException {
		this.status.write(out);
		this.date.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		this.status.readFields(in);
		this.date.readFields(in);
	}

	public Text getStatus() {
		return status;
	}

	public void setStatus(Text status) {
		this.status = status;
	}

	public Text getDate() {
		return date;
	}

	public void setDate(Text date) {
		this.date = date;
	}
	
}
