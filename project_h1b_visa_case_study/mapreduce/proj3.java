import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class proj3{
	
	public static class mapclass extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		public void map(LongWritable key,Text val,Context context) throws IOException, InterruptedException
		{
			try{
			String[] record=val.toString().toUpperCase().split("\t");
			String soc_name=record[3];
			String job_title=record[4];
			String case_status=record[1];
			if(case_status.equals("CERTIFIED") && job_title.equals("DATA SCIENTIST"))
					{
					context.write(new Text(soc_name),new IntWritable(1));
					}
			}
			catch(Exception e)
			{
				System.out.println(e);
			}
		}
	}
	
	public static class reduceclass extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		int maxcount=0;
		String maxkey="";
		public void reduce(Text key,Iterable<IntWritable> val,Context context) throws IOException, InterruptedException
		{
			
			int count=0;
			for(IntWritable v:val)
			{
					count+=v.get();
					if(count>maxcount)
					{
						maxcount=count;
						maxkey=key.toString();
					}
					
			}
			
		//	String result = str + "," + max;
			//context.write(key,new IntWritable(count));
		}
		
		 protected void cleanup(Context context) throws IOException,InterruptedException
	       {
			 context.write(new Text(maxkey),new IntWritable(maxcount));
	       }
	}
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf=new Configuration();
	    Job job = Job.getInstance(conf, "H1b_Visa 6th question");
	    job.setJarByClass(proj3.class);
	    job.setMapperClass(mapclass.class);
	    job.setReducerClass(reduceclass.class);
	    job.setNumReduceTasks(1);
	   // job.setMapOutputKeyClass(Text.class);
	   // job.setMapOutputValueClass(Text.class);
	   job.setCombinerClass(reduceclass.class);
	  //  job.setPartitionerClass(part.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
