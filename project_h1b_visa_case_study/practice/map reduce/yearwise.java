

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class yearwise {
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	
	{
		
		public void map(LongWritable key,Text value,Context context)
		{
			try
			{
			String[] str =value.toString().split(",");
			
			String id=new String(str[0]);
			String year=new String(str[2]);
			
			
				
				
				context.write(new Text(year), new Text(id));
				
			
			
			
			
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
			
			
		}
		
	}

	public static class ReduceClass extends Reducer<Text,Text,Text,IntWritable>
	{
		IntWritable result=new IntWritable();
		
		public void reduce(Text key,Iterable<Text> value,Context context)throws IOException,InterruptedException
		{
			
			int sum=0;
			for(Text val:value)
			{
				
				
				
				sum+=1;
				
				
				
				
			}
			
			result.set(sum);
			
				context.write(key, result);
			
			
			
		}
	}
	 public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
		    Job job = Job.getInstance(conf, "Volume Count");
		    job.setJarByClass(yearwise.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    job.setNumReduceTasks(1);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    job.setOutputKeyClass(NullWritable.class);
		    job.setOutputValueClass(IntWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);

}
}

