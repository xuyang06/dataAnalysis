import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class TweetCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	private static final int MISSING = 9999;
	
	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException{
		HashMap<String, Integer> tokenNumberMap = new HashMap<String, Integer>();
		tokenNumberMap.put("hackathon", 0);
		tokenNumberMap.put("dec", 0);
		tokenNumberMap.put("chicago", 0);
		tokenNumberMap.put("java", 0);
		
		String line = value.toString();
		String[] tweetArray = line.split(",",4);
		String[] tweetArrayItem0Array = tweetArray[0].split("-");
		formTokenNumberMap(tokenNumberMap, tweetArrayItem0Array);
		String[] tweetArrayItem1Array = {tweetArray[1]};
		formTokenNumberMap(tokenNumberMap, tweetArrayItem1Array);
		String[] tweetArrayItem2Array = {tweetArray[2]};
		formTokenNumberMap(tokenNumberMap, tweetArrayItem2Array);
		String[] tweetArrayItem3Array = tweetArray[3].split("-|/|:| ");
		formTokenNumberMap(tokenNumberMap, tweetArrayItem3Array);
		for(String itemKey: tokenNumberMap.keySet()){
			context.write(new Text(itemKey), new IntWritable(tokenNumberMap.get(itemKey)));
		}
	}
	
	public void formTokenNumberMap(HashMap<String, Integer> tokenNumberMap, String[] strItemArray){
		for(int i = 0; i < strItemArray.length; i ++){
			String strItem = strItemArray[i].trim();
			if (strItem.endsWith(",")){
				strItem = strItem.substring(0, strItem.length()-1);
			}
			strItem = strItem.toLowerCase();
			if (tokenNumberMap.containsKey(strItem)){
				tokenNumberMap.put(strItem, 1);
			}
		}
	}

}
