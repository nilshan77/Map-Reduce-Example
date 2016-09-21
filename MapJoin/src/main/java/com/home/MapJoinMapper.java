package com.home;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapJoinMapper extends Mapper<LongWritable, Text, Text, Text>
{
	private Map<Integer,String> deptMap = new HashMap<Integer, String>();
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		String[] recordFields = value.toString().split("\\t");
        int empId = Integer.parseInt(recordFields[0]);
                                               
        String empName = recordFields[1];
        String empSalary = recordFields[2];
                                               
        String deptName = deptMap.get(empId);
                              
        Text keyOut = new Text(String.valueOf(empId));
        Text valueOut = new Text(empName+"--"+empSalary+"--"+deptName);
       
        context.write(keyOut, valueOut);
	}
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		File joinFile = new File(context.getCacheFiles()[0]);
		
		List<String> lines = FileUtils.readLines(joinFile);
	
		for (String line : lines) {
            String[] recordFields = line.split("\\t");
            int key = Integer.parseInt(recordFields[0]);
            String deptName = recordFields[1];
            deptMap.put(key, deptName);
        }		
	}
}
