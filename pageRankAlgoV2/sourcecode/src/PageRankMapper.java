import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class PageRankMapper 
	extends Mapper<LongWritable, Text, Text, Text>{
	
	private static final int MISSING = 9999;
	
	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException{
		HashMap<String, String> tokenNumberMap = new HashMap<String, String>();
		
		String line = value.toString();
		String[] lineArray = line.split("\t| ");
		if (lineArray.length > 2){
			int neighborNum = lineArray.length - 2;
			float prVal = Float.parseFloat(lineArray[lineArray.length - 1]);
			String rootPage = lineArray[0];
			float eachPRVal = prVal/neighborNum;
			String neighborVal = rootPage + "," + String.valueOf(eachPRVal);
			for(int i = 1; i <= neighborNum; i ++){
				context.write(new Text(lineArray[i]), new Text(neighborVal));
			}
			String rootVal = "";
			for(int i = 1; i <= neighborNum; i ++){
				if (i != neighborNum){
					rootVal = rootVal + lineArray[i] + " ";
				}else{
					rootVal = rootVal + lineArray[i];
				}
			}
			context.write(new Text(rootPage), new Text(rootVal));
		}
	}
	
}