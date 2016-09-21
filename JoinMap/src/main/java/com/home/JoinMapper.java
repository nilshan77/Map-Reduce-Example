package com.home;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class JoinMapper extends Mapper<LongWritable, Text, FlightKey, FlightValue> {
	Map<String, WeatherData> data = new HashMap<String, WeatherData>();

	FlightKey fkey = new FlightKey();
	FlightValue fVal = new FlightValue();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlightKey, FlightValue>.Context context)
			throws IOException, InterruptedException {

		System.out.println("Value is :"+value);
		String[] fields = value.toString().split("\\t");

		String year = fields[0];
		String month = fields[1];
		String dayOfMonth = fields[2];
		String deptTime = fields[3];
		String arrTime = fields[4];
		String uniqueCarrier = fields[5];
		String flightNum = fields[6];
		String actElpsTime = fields[7];
		String arrDelay = fields[8];
		String deptDelay = fields[9];
		String origin = fields[10];
		String dest = fields[11];

		String joinKey = dest + year + month + dayOfMonth;

		System.out.println("data contains:"+data.containsKey(joinKey)+" "+joinKey);
		
		if (data.containsKey(joinKey)) {

			System.out.println("In data contains :"+joinKey);
			
			WeatherData wdata = data.get(joinKey);

			fkey.set(year, month, dayOfMonth, arrDelay);

			fVal.set(deptTime, arrTime, uniqueCarrier, flightNum, actElpsTime, arrDelay, deptDelay, origin, dest,
					wdata);

			context.write(fkey, fVal);
		}
	}

	@Override
	protected void setup(Mapper<LongWritable, Text, FlightKey, FlightValue>.Context context)
			throws IOException, InterruptedException {

		//1 File file = new File(context.getCacheFiles()[0]);
		
		URI[] files = DistributedCache.getCacheFiles(context.getConfiguration());
		FileSystem fs = FileSystem.getLocal(context.getConfiguration());

		
		//1 List<String> lines = FileUtils.readLines(new File(files[0]));

		BufferedReader cacheReader = new BufferedReader(new InputStreamReader(fs.open(new Path(files[0]))));
		String line = null;
		while ( (line = cacheReader.readLine()) != null) {
		//1 for (String line : lines) {
			String[] fields = line.split("\\t");
			String dest = fields[0];
			String year = fields[1];
			String month = fields[2];
			String day = fields[3];
			String prcp = fields[4];
			String tmax = fields[5];
			String tmin = fields[6];

			System.out.println("key in data :"+ (dest + year + month + day));
			WeatherData wdata = new WeatherData(dest, year, month, day, prcp, tmax, tmin);
			data.put(dest + year + month + day, wdata);
		}
		System.out.println("Date Size"+data.size());
	}

}
