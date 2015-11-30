import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PageRank {
	public static void main(String[] args) throws Exception {
		
		String paths = "/user/cloudera/00";
		String path1 = paths;
		String path2 = "";
		
		for(int i = 1; i <= 3; i ++){
			System.out.println("Now exectuing the "+i+"-th job!");
			Job job = new Job();
			path2 = paths + i;
			job.setJarByClass(PageRank.class);
			job.setJobName("PageRank");
			path2 = paths + i;
			FileInputFormat.addInputPath(job, new Path(path1));
			FileOutputFormat.setOutputPath(job, new Path(path2));
			
			job.setMapperClass(PageRankMapper.class);
			job.setReducerClass(PageRankReducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			path1 = path2;
			job.waitForCompletion(true);
		}
		//System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
