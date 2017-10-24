import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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



public class postxt{
	
	
	public static class MyMapper extends Mapper<LongWritable,Text, Text, Text> {
        
		
		private Map<String, String> abMap = new HashMap<String, String>();
		
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
		protected void setup(Context context) throws java.io.IOException, InterruptedException{
			
			super.setup(context);

		    URI[] files = context.getCacheFiles(); // getCacheFiles returns null

		    Path p = new Path(files[0]);
		
		   
		    
		    FileSystem fs = FileSystem.get(context.getConfiguration());		    
		
			if (p.getName().equals("store_master")) {
//					BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
					BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(p)));

					String line = reader.readLine();
					while(line != null) {
						String[] tokens = line.split(",");
						String store_id = tokens[0];
						String state = tokens[2];
						abMap.put(store_id, state);
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
        	
        	
        	String row = value.toString();//reading the data from Employees.txt
        	String[] tokens = row.split(",");
        	String store_id = tokens[0];
        	String pro_id=tokens[1];
        	String state = abMap.get(store_id);
        	String qty=tokens[2];
        	String myval=state +","+qty;
        	 
        	outputKey.set(pro_id);
        	outputValue.set(myval);
      	  	context.write(outputKey,outputValue);
        }  
}

	   public static class ReduceClass extends Reducer<Text,Text,Text,Text>
	   {
	     
	     Text myval=new Text();
	     
	      public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException
	      {
	       String word="";
				int sum=0;
	         for (Text val : values)
	         {
	        	 String[] str=val.toString().split(",");
	        	 IntWritable qty=new IntWritable(Integer.parseInt(str[1]));
	        	 sum+=qty.get();
	        	 word=str[0];
	        	 
//	        	         }	
	         }
	         String num=String.format("%d", sum);
	         String num1=word+","+num;
	         myval.set(num1);
				
	        context.write(key,myval);
	      }
	   }
	   
	
	   public static class CaderPartitioner extends
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
	   }
  public static void main(String[] args) 
                  throws IOException, ClassNotFoundException, InterruptedException {
    
	Configuration conf = new Configuration();
	conf.set("mapreduce.output.textoutputformat.separator", ",");
	Job job = Job.getInstance(conf);
    job.setJarByClass(postxt.class);
    job.setJobName("Map Side Join");
    job.setMapperClass(MyMapper.class);
    job.addCacheFile(new Path(args[1]).toUri());
    
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setPartitionerClass(CaderPartitioner.class);
    job.setReducerClass(ReduceClass.class);
    import java.io.IOException;

    import org.apache.hadoop.conf.Configuration;
    import org.apache.hadoop.fs.Path;
    import org.apache.hadoop.io.Text;
    import org.apache.hadoop.io.LongWritable;
    import org.apache.hadoop.mapreduce.Job;
    import org.apache.hadoop.mapreduce.Mapper;
    import org.apache.hadoop.mapreduce.Reducer;
    import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
    import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
    import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

    public class reducejoin {

    	public static class CustsMapper extends
    			Mapper<LongWritable, Text, Text, Text> {
    		public void map(LongWritable key, Text value, Context context)
    				throws IOException, InterruptedException {
    			String record = value.toString();
    			String[] parts = record.split(",");
    			context.write(new Text(parts[0]), new Text("custs\t" + parts[1]));
    		}
    	}

    	public static class TxnsMapper extends
    			Mapper<LongWritable, Text, Text, Text> {
    		public void map(LongWritable key, Text value, Context context)
    				throws IOException, InterruptedException {
    			String record = value.toString();
    			String[] parts = record.split(",");
    			context.write(new Text(parts[2]), new Text("txns\t" + parts[3]));
    		}
    	}

    	public static class ReduceJoinReducer extends
    			Reducer<Text, Text, Text, Text> {
    		public void reduce(Text key, Iterable<Text> values, Context context)
    				throws IOException, InterruptedException {
    			String name = "";
    			double total = 0.0;
    			int count = 0;
    			for (Text t : values) {
    				String parts[] = t.toString().split("\t");
    				if (parts[0].equals("txns")) {
    					count++;
    					total += Float.parseFloat(parts[1]);
    				} else if (parts[0].equals("custs")) {
    					name = parts[1];
    				}
    			}
    			String str = String.format("%d\t%f", count, total);
    			context.write(new Text(name), new Text(str));
    		}
    	}

    	public static void main(String[] args) throws Exception {
    		
    		Configuration conf = new Configuration();
    		Job job = Job.getInstance(conf);
    	    job.setJarByClass(reducejoin.class);
    	    job.setJobName("Reduce Side Join");
    		job.setReducerClass(ReduceJoinReducer.class);
    		job.setOutputKeyClass(Text.class);
    		job.setOutputValueClass(Text.class);
    		//job.setNumReduceTasks(0);
    		MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, CustsMapper.class);
    		MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, TxnsMapper.class);
    		
    		Path outputPath = new Path(args[2]);
    		FileOutputFormat.setOutputPath(job, outputPath);
    		//outputPath.getFileSystem(conf).delete(outputPath);
    		
    		System.exit(job.waitForCompletion(true) ? 0 : 1);
    	}
    }
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    /*job.setInputFormatClass(TextInputFormat.class);
	
    job.setOutputFormatClass(TextOutputFormat.class);*/
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.waitForCompletion(true);
    
    
  }
}