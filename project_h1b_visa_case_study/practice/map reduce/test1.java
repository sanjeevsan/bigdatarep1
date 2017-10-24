
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

public class test1 {
	
	
	public static class MyMapper extends Mapper<LongWritable,Text, Text, Text> {
        
		
		private Map<String, String> abMap = new HashMap<String, String>();
		private Map<String, String> abMap1 = new HashMap<String, String>();
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
		protected void setup(Context context) throws java.io.IOException, InterruptedException{
			
			super.setup(context);

		    URI[] files = context.getCacheFiles(); // getCacheFiles returns null

		    Path p = new Path(files[0]);
		
		    FileSystem fs = FileSystem.get(context.getConfiguration());		    
				   
		
			if (p.getName().equals("results.dat")) {
			//	BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
				BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(p)));
				String line = reader.readLine();
					while(line != null) {
						String[] tokens = line.split("||");
						String id = tokens[0];
						String result = tokens[1];
						if(result.contains("pass"))
						{
						abMap.put(id, result);
						}
						line = reader.readLine();
					}
					reader.close();
				}
			
			
			if (abMap.isEmpty()) {
				throw new IOException("MyError:Unable to load salary data.");
			}

		}

		

		
        protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {
        	
        	
        	String row = value.toString();//reading the data from Employees.txt
        	String[] tokens = row.split("||");
        	String id = tokens[1];
        	String result = abMap.get(id);
            String name=tokens[0];
        	String final1 = result + "," + name; 
        
      	  	context.write(new Text(id),new Text(final1));
        }  
}
	
	
  public static void main(String[] args) 
                  throws IOException, ClassNotFoundException, InterruptedException {
    
	Configuration conf = new Configuration();
	conf.set("mapreduce.output.textoutputformat.separator", ",");
	Job job = Job.getInstance(conf);
    job.setJarByClass(test1.class);
    job.setJobName("Map Side Join");
    job.setMapperClass(MyMapper.class);
    job.addCacheFile(new Path(args[1]).toUri());
    
    job.setNumReduceTasks(0);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    
    job.waitForCompletion(true);
    
    
  }
}