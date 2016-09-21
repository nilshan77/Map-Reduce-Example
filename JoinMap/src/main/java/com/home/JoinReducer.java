package com.home;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<FlightKey, FlightValue, NullWritable, Text> {
	
	
	
	@Override
	protected void reduce(FlightKey key, Iterable<FlightValue> value,
			Reducer<FlightKey, FlightValue, NullWritable, Text>.Context context) throws IOException, InterruptedException {

		System.out.println("Key in Reduce :"+key);
		StringBuffer sb = new StringBuffer();
		
		sb.append(key.getYear().get()+", ");
		sb.append(key.getMonth().get()+", ");
		sb.append(key.getDay().get()+", ");
		
		FlightValue val = value.iterator().next();
		
		sb.append(val.getDepTime()+", ");
		sb.append(val.getArrTime()+", ");
		sb.append(val.getUniqueCarrier()+", ");
		sb.append(val.getFlightNum()+", ");
		sb.append(val.getActualElapsedTime()+", ");
		sb.append(val.getArrDelay()+", ");
		sb.append(val.getDeptDalay()+", ");
		sb.append(val.getOrigin()+", ");
		sb.append(val.getDest()+", ");
		sb.append(val.getPrcp()+", ");
		sb.append(val.getTmax()+", ");
		sb.append(val.getTmin());
		
		context.write(NullWritable.get(), new Text(sb.toString()));
		
	}
}
