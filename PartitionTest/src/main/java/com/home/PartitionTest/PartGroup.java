package com.home.PartitionTest;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PartGroup extends WritableComparator {

	public PartGroup() {
		super(org.apache.hadoop.io.Text.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		Text a1 = (Text) a;
		Text b1 = (Text) b;
		return a1.compareTo(b1);
	}
}
