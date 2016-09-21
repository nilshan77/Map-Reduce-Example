package com.home.Srjoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class ServiceDetails implements Writable {

	private Text contactName, companyName, phNum;

	public ServiceDetails() {
		this.contactName = new Text();
		this.companyName = new Text();
		this.phNum = new Text();
	}

	public void set(String contactNm, String companyNm, String ph) {
		this.contactName.set(contactNm);
		this.companyName.set(companyNm);
		this.phNum.set(ph);
	}

	public void write(DataOutput out) throws IOException {
		this.contactName.write(out);
		this.companyName.write(out);
		this.phNum.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		this.contactName.readFields(in);
		this.companyName.readFields(in);
		this.phNum.readFields(in);
	}

	public Text getContactName() {
		return contactName;
	}

	public void setContactName(Text contactName) {
		this.contactName = contactName;
	}

	public Text getCompanyName() {
		return companyName;
	}

	public void setCompanyName(Text companyName) {
		this.companyName = companyName;
	}

	public Text getPhNum() {
		return phNum;
	}

	public void setPhNum(Text phNum) {
		this.phNum = phNum;
	}
}
