package com.home.ReduceJoinExample;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CompanyIdKey implements WritableComparable<CompanyIdKey> {

	private IntWritable id;
	private Text keyType;

	public static String COMPANYINFORECORD = "COMPANYINFORECORD";
	public static String COMPANYLOCRECORD = "COMPANYLOCRECORD";

	public CompanyIdKey() {
		id = new IntWritable();
		keyType = new Text();
	}

	public void set(int id, String type) {
		this.id.set(id);
		this.keyType.set(type);
	}

	public void write(DataOutput out) throws IOException {
		id.write(out);
		keyType.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		id.readFields(in);
		keyType.readFields(in);
	}

	public int compareTo(CompanyIdKey nextkey) {
		if (this.getId().equals(nextkey.getId())) {
			return this.getKeyType().compareTo(nextkey.getKeyType());
		}
		return this.getId().compareTo(nextkey.getId());
	}

	public IntWritable getId() {
		return id;
	}

	public void setId(IntWritable id) {
		this.id = id;
	}

	public Text getKeyType() {
		return keyType;
	}

	public void setKeyType(Text keyType) {
		this.keyType = keyType;
	}

}
