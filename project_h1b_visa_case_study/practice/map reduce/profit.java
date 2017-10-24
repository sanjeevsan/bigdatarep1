
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class profit {
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	
	{
		public void map(LongWritable key,Text value,Context context)
		{
			try
			{
			String[] str =value.toString().split(";");
			
			String id=new String(str[4]);
			String cost=new String(str[7]);
			String sales=new String(str[8]);
			String myvalue=cost +","+sales;
			context.write(new Text(id), new Text(myvalue));
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
			
			
		}
		
	}

	public static class ReduceClass extends Reducer<Text,Text,Text,LongWritable>
	{
		LongWritable result=new LongWritable();
		Long result1;
		long sum=0;
		
		public void reduce(Text key,Iterable<Text> value,Context context)
		{
			
			
			for(Text val:value)
			{
				String[] str=val.toString().split(",");
				Long num1=Long.parseLong(str[0]);
				Long num2=Long.parseLong(str[1]);
				 result1=num2-num1;
				 sum+=result1;
				
				
				
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
		    //conf.set("name", "value")
		    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
		    Job job = Job.getInstance(conf, "Volume Count");
		    job.setJarByClass(profit.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    job.setNumReduceTasks(1);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(LongWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);

}
}
