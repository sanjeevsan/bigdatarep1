
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class testing {

	public static class TokenizerMapper
       extends Mapper<LongWritable, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text sentence = new Text();
    
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	String mySearchText = context.getConfiguration().get("myText");
    	String mySearchText1 = context.getConfiguration().get("mytext1");
    	String line = value.toString();
        String newline = line.toLowerCase();
    	String newText = mySearchText.toLowerCase();

    	if(mySearchText != null)
      {
    	  if(newline.contains(newText))
    	  {
    		  
    	  else
    	  
    		  if(newline.contains(mySearchText1))
    		  {
    		  sentence.set(newline);
    		  context.write(sentence, one);
         }
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
	
	Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator","|");
	
	//System.out.println(args.length);
	//System.out.println(args[2]);
	  if(args.length > 2)
      {
		  conf.set("myText", args[2]);
		  conf.set("mytext1", args[3]);
      }
	  else
	  {
		  System.out.println("Number of arguments should be 3");
		  System.exit(0);
	  }
	Job job = Job.getInstance(conf, "String Search");

	job.setJarByClass(testing.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    //job.setNumReduceTasks(0);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
}