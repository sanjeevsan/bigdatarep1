import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;

import org.apache.hadoop.fs.*;

import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import org.apache.hadoop.util.*;

public class projectc 
{
   //Map class
	
   public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
   {
      public void map(LongWritable key, Text value, Context context)
      {
         try{
            String[] str = value.toString().split(";");
            String age=str[2];
            int  sales =Integer.parseInt(str[8]);
            int cost = Integer.parseInt(str[7]);
            int profit = sales-cost;
            String subclass=str[4];
            if(profit > 0)
            {
            String myValue = profit + "," + age ;
            context.write(new Text(subclass), new Text(myValue));
         }
         }
         catch(Exception e)
         {
            System.out.println(e.getMessage());
         }
      }
   }
   
   //Reducer class
	
   public static class ReduceClass extends Reducer<Text,Text,Text,Text>
   {
  
      
      public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException
      {
         
			
         for (Text val : values)
         {
        	  context.write(new Text(key), new Text(val));
//        	
         }
			
       
      }
   }
   
   //Partitioner class
	
   public static class CaderPartitioner extends
   Partitioner < Text, Text >
   {
      @Override
      public int getPartition(Text key, Text value, int numReduceTasks)
      {
         
         String[] val=value.toString().split(",");
         String val1=val[1];
         
         
         if(val1.contains("A"))
         {
            return 0;
         }
         else if(val1.equals("B"))
         {
            return 1 ;
         }
         else if(val1.equals("C"))
         {
            return 2;
         }
         else if(val1.equals("D"))
        		 {
        	 return 3;
        		 }
         else if(val1.equals("E"))
         {
        	 return 4;
         }
         else if(val1.equals("F"))
         {
        	 return 5;
         }
         else if(val1.equals("G"))
         {
        	 return 6;
        			 }
         else if(val1.equals("H"))
         {
        	 return 7;
         }
         else if(val1.equals("I"))
         {
        	 return 8;
         }
         else 
         {
        	 return 9;
         }
		
      }
   }
   

   public static void main(String ar[]) throws Exception
   {
	
	   
	  Configuration conf = new Configuration();
	  Job job = Job.getInstance(conf);
	  job.setJarByClass(projectc.class);
	  
      FileInputFormat.setInputPaths(job, new Path(ar[0]));
      FileOutputFormat.setOutputPath(job,new Path(ar[1]));
		
      job.setMapperClass(MapClass.class);
		
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      
      //set partitioner statement
		
    job.setPartitionerClass(CaderPartitioner.class);
      job.setReducerClass(ReduceClass.class);
      job.setNumReduceTasks(10);
      job.setInputFormatClass(TextInputFormat.class);
		
      job.setOutputFormatClass(TextOutputFormat.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
		
      System.exit(job.waitForCompletion(true)? 0 : 1);
      
   }
}
   
   
   