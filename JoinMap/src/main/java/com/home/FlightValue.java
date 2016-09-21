package com.home;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class FlightValue implements Writable {

	Text depTime;
	Text arrTime;
	Text uniqueCarrier;
	Text flightNum;
	Text actualElapsedTime;
	Text arrDelay;
	Text deptDalay;
	Text origin;
	Text dest;
	Text prcp;
	Text tmax;
	Text tmin;

	public FlightValue() {
		depTime = new Text();
		arrTime = new Text();
		uniqueCarrier = new Text();
		flightNum = new Text();
		actualElapsedTime = new Text();
		arrDelay = new Text();
		deptDalay = new Text();
		origin = new Text();
		dest = new Text();
		prcp = new Text();
		tmax = new Text();
		tmin = new Text();
	}

	public void write(DataOutput out) throws IOException {
		depTime.write(out);
		arrTime.write(out);
		uniqueCarrier.write(out);
		flightNum.write(out);
		actualElapsedTime.write(out);
		arrDelay.write(out);
		deptDalay.write(out);
		origin.write(out);
		dest.write(out);
		prcp.write(out);
		tmax.write(out);
		tmin.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		depTime.readFields(in);
		arrTime.readFields(in);
		uniqueCarrier.readFields(in);
		flightNum.readFields(in);
		actualElapsedTime.readFields(in);
		arrDelay.readFields(in);
		deptDalay.readFields(in);
		origin.readFields(in);
		dest.readFields(in);
		prcp.readFields(in);
		tmax.readFields(in);
		tmin.readFields(in);
	}

	public void set(String deptTime, String arrTime, String uc, String fn, String aet, String ad, String dd, String og,
			String dest, WeatherData d) {
		this.depTime.set(deptTime);
		this.arrTime.set(arrTime);
		this.uniqueCarrier.set(uc);
		this.getFlightNum().set(fn);
		this.actualElapsedTime.set(aet);
		this.arrDelay.set(ad);
		this.deptDalay.set(dd);
		this.origin.set(og);
		this.dest.set(dest);
		this.prcp.set(d.getPrcp());
		this.tmax.set(d.getTmax());
		this.tmin.set(d.getTmin());
	}

	public Text getDepTime() {
		return depTime;
	}

	public void setDepTime(Text depTime) {
		this.depTime = depTime;
	}

	public Text getArrTime() {
		return arrTime;
	}

	public void setArrTime(Text arrTime) {
		this.arrTime = arrTime;
	}

	public Text getUniqueCarrier() {
		return uniqueCarrier;
	}

	public void setUniqueCarrier(Text uniqueCarrier) {
		this.uniqueCarrier = uniqueCarrier;
	}

	public Text getFlightNum() {
		return flightNum;
	}

	public void setFlightNum(Text flightNum) {
		this.flightNum = flightNum;
	}

	public Text getActualElapsedTime() {
		return actualElapsedTime;
	}

	public void setActualElapsedTime(Text actualElapsedTime) {
		this.actualElapsedTime = actualElapsedTime;
	}

	public Text getArrDelay() {
		return arrDelay;
	}

	public void setArrDelay(Text arrDelay) {
		this.arrDelay = arrDelay;
	}

	public Text getDeptDalay() {
		return deptDalay;
	}

	public void setDeptDalay(Text deptDalay) {
		this.deptDalay = deptDalay;
	}

	public Text getOrigin() {
		return origin;
	}

	public void setOrigin(Text origin) {
		this.origin = origin;
	}

	public Text getDest() {
		return dest;
	}

	public void setDest(Text dest) {
		this.dest = dest;
	}

	public Text getPrcp() {
		return prcp;
	}

	public void setPrcp(Text prcp) {
		this.prcp = prcp;
	}

	public Text getTmax() {
		return tmax;
	}

	public void setTmax(Text tmax) {
		this.tmax = tmax;
	}

	public Text getTmin() {
		return tmin;
	}

	public void setTmin(Text tmin) {
		this.tmin = tmin;
	}
	
}
