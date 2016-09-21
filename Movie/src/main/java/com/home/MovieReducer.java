package com.home;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieReducer extends Reducer<MovieKey, MovieValue, Text, Text> {

	@Override
	protected void reduce(MovieKey movieKey, Iterable<MovieValue> movieList, Context context)
			throws IOException, InterruptedException {
		System.out.println("In a Reducer");
		float maxEarning = -1;
		String name = "";
		String details = "";
		
		System.out.println("Key:-->"+movieKey.toString());
		
		for(MovieValue mv : movieList){
			System.out.println("-->"+mv.toString());
			if(mv.getGrossEarning().get() > maxEarning)
			{
				maxEarning = mv.getGrossEarning().get();
				name = mv.getName().toString();
				details = "Year :"+ mv.getYear() +", Ratings :" + mv.getRatings() + ", Earnings :"+mv.getGrossEarning()+" Crore";
				details = details + ", Genre :"+mv.getGenre(); 
			}
		}
		context.write(new Text(name), new Text(details));
	}
}
