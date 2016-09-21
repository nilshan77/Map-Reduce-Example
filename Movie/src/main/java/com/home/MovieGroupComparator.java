package com.home;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MovieGroupComparator extends WritableComparator {

	public MovieGroupComparator() {
		super(MovieKey.class,true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		System.out.println("In a Group");
		MovieKey firstMovie = (MovieKey) a;
		MovieKey secMovie = (MovieKey) b;
		
		return firstMovie.getGenre().compareTo(secMovie.getGenre());
	}
}
