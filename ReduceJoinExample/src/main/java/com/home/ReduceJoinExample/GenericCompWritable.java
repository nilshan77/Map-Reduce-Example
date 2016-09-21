package com.home.ReduceJoinExample;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

@SuppressWarnings("unchecked")
public class GenericCompWritable extends GenericWritable {

	private static Class<? extends Writable>[] CLASSES;

	static {
		CLASSES = (Class<? extends Writable>[]) new Class[] { CompanyRecord.class, CompanyLocationRecord.class };
	}

	public GenericCompWritable() {
	}

	public GenericCompWritable(Writable instance) {
		set(instance);
	}

	@Override
	protected Class<? extends Writable>[] getTypes() {
		return CLASSES;
	}

}
