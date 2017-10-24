import java.io.BufferedReader;

import java.io.InputStreamReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;



public class sentimental{
	
	
	public static class MyMapper extends Mapper<LongWritable,Text, Text, IntWritable> {
        
		String myword="";
		int myval=0;
		
		private Map<String, String> abMap = new HashMap<String, String>();
		
		private Text outputKey = new Text();
		private IntWritable outputValue = new IntWritable();
		
		protected void setup(Context context) throws java.io.IOException, InterruptedException{
			
			super.setup(context);

		    URI[] files = context.getCacheFiles(); // getCacheFiles returns null

		    Path p = new Path(files[0]);
		
		   
		    
		    FileSystem fs = FileSystem.get(context.getConfiguration());		    
		
			if (p.getName().equals("AFINN.txt")) {
//					BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
					BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(p)));

					String line = reader.readLine();
					while(line != null) {
						String[] tokens = line.split("\t");
						String word= tokens[0];
						String num = tokens[1];
						abMap.put(word, num);
						line = reader.readLine();
					}
					reader.close();
				}
			/*if (p1.getName().equals("desig.txt")) {
//				BufferedReader reader = new BufferedReader(new FileReader(p1.toString()));
				BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(p1)));
				String line = reader.readLine();
				while(line != null) {
					String[] tokens = line.split(",");
					String emp_id = tokens[0];
					String emp_desig = tokens[1];
					abMap1.put(emp_id, emp_desig);
					line = reader.readLine();
				}
				reader.close();
			}*/
		
			
			if (abMap.isEmpty()) {
				throw new IOException("MyError:Unable to load salary data.");
			}

			/*if (abMap1.isEmpty()) {
				throw new IOException("MyError:Unable to load designation data.");
			}*/

		}
		

		
        protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {
        	
        	
        	StringTokenizer str=new StringTokenizer(value.toString());
        	while(str.hasMoreTokens())
        	{
        		
        		myword=str.nextToken().toLowerCase();
        		
        		if(abMap.get(myword)!=null)
        		{
        			myval=Integer.parseInt(abMap.get(myword));
        			if(myval > 0)
        			{
        				myword="positive";
        				
        			}
        			 if(myval < 0)
        			{
        				myword="negative";
        				myval*=-1;
        			}
        		}
        			else
        			{
        				myword="positive";
        				myval=0;
        			}
        		outputKey.set(myword);
            	outputValue.set(myval);
            	context.write(outputKey,outputValue);
        		}
        		
        		
        	
        	
        	
        }  
}

	   public static class ReduceClass extends Reducer<Text,Text,NullWritable,DoubleWritable>
	   {
	     
	     Text myval=new Text();
	     public int pos_total=0;
	     public int neg_total=0;
	     DoubleWritable result1=new DoubleWritable();
	     
	     
	      public void reduce(Text key, Iterable <IntWritable> values, Context context) throws IOException, InterruptedException
	      {
	      int sum=0;
	    
	         for (IntWritable val : values)
	         {
	        	 
	        	 sum+=val.get();
//	        	         }	
	         }
	         if(key.toString().equals("positive"))
	         {
	        	 pos_total+=sum;
	         }
	         else
	        	 neg_total+=sum;
	       
	   }
	      protected void cleanup(Context context) throws IOException,InterruptedException
	         {
	    	  double result=(((double)pos_total)-(double)neg_total)/((double)pos_total+(double)neg_total)*100;
	    	  result1.set(result);
	    	  context.write(NullWritable.get(), result1);
	         }
	
	   /*public static class CaderPartitioner extends
	   Partitioner < Text, Text >
	   {
	      @Override
	      public int getPartition(Text key, Text value, int numReduceTasks)
	      {
	         String[] str = value.toString().split(",");
	         String state=str[0];
	         
	         
	         if(state.equals("MAH"))
	         {
	            return 0;
	         }
	         else 
	         {
	            return 1 ;
	         }
	      
	      }
	   }*/
  public static void main(String[] args) 
                  throws IOException, ClassNotFoundException, InterruptedException {
    
	Configuration conf = new Configuration();
	conf.set("mapreduce.output.textoutputformat.separator", ",");
	Job job = Job.getInstance(conf);
    job.setJarByClass(sentimental.class);
    job.setJobName("Map Side Join");
    job.setMapperClass(MyMapper.class);
    job.addCacheFile(new Path(args[1]).toUri());
    
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    
    job.setReducerClass(ReduceClass.class);
    job.setNumReduceTasks(1);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    /*job.setInputFormatClass(TextInputFormat.class);
	
    job.setOutputFormatClass(TextOutputFormat.class);*/
    //job.setOutputKeyClass(Text.class);
    //job.setOutputValueClass(IntWritable.class);
    job.waitForCompletion(true);
    
    
  }
}