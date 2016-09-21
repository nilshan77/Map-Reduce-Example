package com.home.ReduceJoinExample;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompanySorting extends WritableComparator {

	public CompanySorting() {
		super(CompanyIdKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		CompanyIdKey first = (CompanyIdKey) a;
		CompanyIdKey second = (CompanyIdKey) b;
		return first.compareTo(second);
	}

}
