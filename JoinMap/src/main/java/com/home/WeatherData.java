package com.home;

public class WeatherData {
	
	private String dest;
	private String year;
	private String month;
	private String day;
	private String prcp;
	private String tmax;
	private String tmin;
	
	public WeatherData() {
	}
	
	public WeatherData(String dt,String y, String m, String d, String prcp, String tmax, String tmin) {
		setDest(dt);
		setYear(y);
		setMonth(m);
		setDay(d);
		setPrcp(prcp);
		setTmax(tmax);
		setTmin(tmin);
	}

	public String getDest() {
		return dest;
	}

	public void setDest(String dest) {
		this.dest = dest;
	}

	public String getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = year;
	}

	public String getMonth() {
		return month;
	}

	public void setMonth(String month) {
		this.month = month;
	}

	public String getDay() {
		return day;
	}

	public void setDay(String day) {
		this.day = day;
	}

	public String getPrcp() {
		return prcp;
	}

	public void setPrcp(String prcp) {
		this.prcp = prcp;
	}

	public String getTmax() {
		return tmax;
	}

	public void setTmax(String tmax) {
		this.tmax = tmax;
	}

	public String getTmin() {
		return tmin;
	}

	public void setTmin(String tmin) {
		this.tmin = tmin;
	}
	
	
}
