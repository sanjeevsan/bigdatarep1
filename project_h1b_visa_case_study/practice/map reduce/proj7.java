

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




public class proj7 {
	public static class MapClass extends Mapper<LongWritable,Text,Text,LongWritable>
	
	{
		 private final static LongWritable one=new LongWritable(1);
		String key1="all";
		public void map(LongWritable key,Text value,Context context)
		{
			try
			{
			String[] str =value.toString().split("\t");
			
			String year=new String(str[7]);
			
			
			
				
				context.write(new Text(year), one);
				
		
			
			
			
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
			
			
		}
		
	}

	public static class ReduceClass extends Reducer<Text,LongWritable,Text,LongWritable>
	{
		IntWritable result=new IntWritable();
		
		public void reduce(Text key,Iterable<LongWritable> value,Context context)throws IOException,InterruptedException
		{
			long count=0;
			int sum=0;
			for(LongWritable val:value)
			{
				
				count+=val.get();
				
				
				
				
				
				
			}
			
			
			
				context.write(new Text(key), new LongWritable(count));
			
			
			
		}
	}
	 public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
		    Job job = Job.getInstance(conf, "Volume Count");
		    job.setJarByClass(proj7.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    job.setNumReduceTasks(1);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(LongWritable.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(LongWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);

}
}

