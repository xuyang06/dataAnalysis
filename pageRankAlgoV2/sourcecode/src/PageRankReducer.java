import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class PageRankReducer extends Reducer<Text, Text, Text, Text>{
	
	@Override
	public void reduce(Text key, Iterable<Text> values, 
			Context context) throws IOException, InterruptedException {
		
		//int totalCount = 0;
		float prTotalVal = 0;
		String neighborStr = "";
		for (Text value : values) {
			String strVal = value.toString();
			if (strVal.length() >= 2 && strVal.charAt(1) == ','){
				float prItem = Float.parseFloat(strVal.substring(2));
				prTotalVal = prTotalVal + prItem;
			}else{
				neighborStr = strVal;
			}
		}
		String val = neighborStr + " " + Float.toString(prTotalVal);
		context.write(key, new Text(val));
	}
}
