
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




public class speed_data {
	public static class MapClass extends Mapper<LongWritable,Text,Text,IntWritable>
	
	{
		public void map(LongWritable key,Text value,Context context)
		{
			try
			{
			String[] str =value.toString().split(",");
			
			String id=new String(str[0]);
			String speed=new String(str[1]);
             int speed1=Integer.parseInt(speed);
             
			context.write(new Text(id), new IntWritable(speed1));
			
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
			
			
		}
		
	}

	public static class ReduceClass extends Reducer<Text,IntWritable,Text,FloatWritable>
	{
		public void reduce(Text key,Iterable<IntWritable> value,Context context)
		{
			float count=0;
			float totalcount=0;
			float ans;
			float ans1;
			FloatWritable result=new FloatWritable();
			
			
			for(IntWritable val:value)
				
			{
				
				int a=val.get();
				totalcount+=1;
				
				if(a > 65)
				{
					count+=1;
					
					
				}
				
				
				
			}
			ans=count/totalcount;
			
			ans1=ans*100;
			result.set(ans1);
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
		    //conf.set("name", "value")
		    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
		    Job job = Job.getInstance(conf, "Volume Count");
		    job.setJarByClass(speed_data.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    job.setNumReduceTasks(0);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(IntWritable.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(FloatWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);

}
}
