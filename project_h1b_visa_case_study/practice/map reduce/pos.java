import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;

import org.apache.hadoop.fs.*;

import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import org.apache.hadoop.util.*;

public class pos 
{
   //Map class
	
   public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
   {
      public void map(LongWritable key, Text value, Context context)
      {
         try{
            String[] str = value.toString().split(",");
            String proid=str[1];
            String stid = str[0];
            String qty = str[2];
            //String salary = str[4];
            String myValue = stid + ',' + qty ;
            context.write(new Text(proid), new Text(myValue));
         }
         catch(Exception e)
         {
            System.out.println(e.getMessage());
         }
      }
   }
   
   //Reducer class
	
   public static class ReduceClass extends Reducer<Text,Text,Text,IntWritable>
   {
      
      private IntWritable outputKey = new IntWritable();
      public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException
      {
       int  sum = 0;
			
         for (Text val : values)
         {
        	
//        	outputKey.set(key);
        	String [] str = val.toString().split(",");
            IntWritable a=new IntWritable(Integer.parseInt(str[1]));
            	
            	sum+=a.get();
            	outputKey.set(sum);

            	
         }
			
         context.write(key, outputKey);
      }
   }
   
   //Partitioner class
	
   public static class CaderPartitioner extends
   Partitioner < Text, Text >
   {
      @Override
      public int getPartition(Text key, Text value, int numReduceTasks)
      {
         String[] str = value.toString().split(",");
         int stid = Integer.parseInt(str[0]);
         
         
         if(stid==21)
         {
            return 0;
         }
         else
         {
            return 1 ;
         }
         
      }
   }
   

   public static void main(String ar[]) throws Exception
   {
	
	   
	  Configuration conf = new Configuration();
	  Job job = Job.getInstance(conf);
	  job.setJarByClass(pos.class);
	  job.setJobName("Top Salaried Employees");
      FileInputFormat.setInputPaths(job, new Path(ar[0]));
      FileOutputFormat.setOutputPath(job,new Path(ar[1]));
		
      job.setMapperClass(MapClass.class);
		
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      
      //set partitioner statement
		
      job.setPartitionerClass(CaderPartitioner.class);
      job.setReducerClass(ReduceClass.class);
      job.setNumReduceTasks(2);
      job.setInputFormatClass(TextInputFormat.class);
		
      job.setOutputFormatClass(TextOutputFormat.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
		
      System.exit(job.waitForCompletion(true)? 0 : 1);
      
   }
}
   
   
   