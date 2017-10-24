import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class training {
	public static class Mape extends Mapper<LongWritable,Text,Text,Text>
	
	{
		public void map(LongWritable key,Text value,Context context)
		{
			try
			{
			String[] str =value.toString().split(" ");
			
			context.write(new Text(str[2]),new Text(str[3]));
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
			
			
		}
		
	}

	public static class Redu extends Reducer<Text,Text,Text,IntWritable>
	{
		IntWritable result=new IntWritable();
		public void reduce(Text key,Iterable<Text> value,Context context)
		{
			
			int sum=0;
			for(Text val:value)
			{
				if(val.equals(" [ERROR] "))
				{
					sum=sum+1;
	
				}
			}
			result.set(sum);
			try {
				context.write(key, result);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(training.class);
	    job.setMapperClass(Mape.class);
	   
	    job.setReducerClass(Redu.class);
	    job.setOutputKeyClass(Text.class);
	    job.setReducerClass(Redu.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);

}
}