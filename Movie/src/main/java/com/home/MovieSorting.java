package com.home;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MovieSorting extends WritableComparator {

	public MovieSorting() {
		super(MovieKey.class,true);
	}
		
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		System.out.println("In a Sorting");
		MovieKey firstMovie = (MovieKey) a;
		MovieKey secMovie = (MovieKey) b;
		
		int cpr = firstMovie.getYear().compareTo(secMovie.getYear()); 
		
		if(cpr == 0)
			cpr = firstMovie.getGenre().compareTo(secMovie.getGenre());
		
		return -1*cpr;
	}
}
