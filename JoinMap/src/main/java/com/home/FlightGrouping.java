package com.home;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FlightGrouping extends WritableComparator {

	public FlightGrouping() {
		super(FlightKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		FlightKey f1 = (FlightKey) a;
		FlightKey f2 = (FlightKey) b;

		if (f1.getYear().compareTo(f2.getYear()) == 0) {
			
			if(f1.getMonth().compareTo(f2.getMonth()) == 0){
			
				if(f1.getDay().compareTo(f2.getDay()) == 0){
					return 0;
				}
				return f1.getDay().compareTo(f2.getDay());
			}
			return f1.getMonth().compareTo(f2.getMonth());
		}
		return f1.getYear().compareTo(f2.getYear());
	}
}
