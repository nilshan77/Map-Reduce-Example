package com.home.Srjoin;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

public class GenericSrWritable extends GenericWritable{

	private Class<? extends Writable>[] clazzes;
	
	@SuppressWarnings("unchecked")
	public GenericSrWritable() {
		clazzes = new Class[] {ServiceRequest.class, ServiceDetails.class};
	}

	@Override
	protected Class<? extends Writable>[] getTypes() {
		return clazzes;
	}

	
}
