package com.home.Srjoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SrGroupComparator extends WritableComparator {

	public SrGroupComparator() {
		super(Text.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {

		String aText = a.toString().split("-")[0];
		String bText = b.toString().split("-")[0];

		return aText.compareTo(bText);
	}
}
