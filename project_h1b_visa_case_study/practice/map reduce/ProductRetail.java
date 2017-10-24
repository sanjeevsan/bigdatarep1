


	import java.io.*;

import org.apache.hadoop.io.NullWritable;
	import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


	public class ProductRetail{
		
		public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
		   {
		      public void map(LongWritable key, Text value, Context context)
		      {	    	  
		         try{
		            String[] str = value.toString().split(";");	 
		            String mykey="All";
		            String cusid=str[1];
		            String dt=str[0];
		            String sales=str[8];
		            String myvalue=cusid + "," + dt +"," + sales ;
		            Text val=new Text(mykey);
		            Text val1=new Text(myvalue);
		            context.write(val, val1);
		            
		         
		         }
		         catch(Exception e)
		         {
		            System.out.println(e.getMessage());
		         }
		      }
		   }
		
		  public static class ReduceClass extends Reducer<Text,Text,NullWritable,Text>
		   {
			    private LongWritable result = new LongWritable();
			    
			    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			      long max = 0;
			 String custid="";
			 String dt="";
					
			         for (Text val : values)
			         {       	
			        	     
			        	 String[] str=val.toString().split(",");
			        	
			        	 
			        	 if(Long.parseLong(str[2])>max)
			        	 {
			        		 max=Long.parseLong(str[2]);
			        		 custid=str[0];
			        		 dt=str[1];
			        	 }
			        	 
			         }
			     
			         String sales2=String.format("%d",max);
			         
			         String myvalue=custid +","+dt +","+max;
			         
			      		      
			      context.write(  NullWritable.get(), new Text(myvalue));
			      //context.write(key, new LongWritable(sum));
			      
			    }
		   }
		  public static void main(String[] args) throws Exception {
			    Configuration conf = new Configuration();
			    //conf.set("name", "value")
			    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
			    Job job = Job.getInstance(conf, "Volume Count");
			    job.setJarByClass(ProductRetail.class);
			    job.setMapperClass(MapClass.class);
			    //job.setCombinerClass(ReduceClass.class);
			    job.setReducerClass(ReduceClass.class);
			    job.setNumReduceTasks(1);
			    job.setMapOutputKeyClass(Text.class);
			    job.setMapOutputValueClass(Text.class);
			    job.setOutputKeyClass(NullWritable.class);
			    job.setOutputValueClass(Text.class);
			    FileInputFormat.addInputPath(job, new Path(args[0]));
			    FileOutputFormat.setOutputPath(job, new Path(args[1]));
			    System.exit(job.waitForCompletion(true) ? 0 : 1);
			  }
	}


	
