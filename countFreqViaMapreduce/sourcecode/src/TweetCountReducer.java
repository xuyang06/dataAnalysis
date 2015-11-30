import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class TweetCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, 
			Context context) throws IOException, InterruptedException {
		int totalCount = 0;
		for (IntWritable value : values) {
			totalCount = totalCount + value.get();
			//maxValue = Math.max(maxValue, value.get());
		}
		context.write(key, new IntWritable(totalCount));
	}
}
