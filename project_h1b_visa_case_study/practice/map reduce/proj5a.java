import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class proj5a {
   
    public static class mapclass extends Mapper<LongWritable,Text,Text,Text>
    {
        public void map(LongWritable key,Text val,Context context) throws IOException, InterruptedException
        {
            try{
            String[] record=val.toString().toUpperCase().split("\t");
            String job_title=record[4];
            String year=record[7];
            String case_status=record[1];
            String value=1+","+year;
            if(case_status.equals("CERTIFIED"))
            {
            context.write(new Text(job_title),new Text(value));
            }
            }
            catch(Exception e)
            {
                System.out.println(e);
            }
        }
    }
   
    public static class part extends Partitioner<Text,Text>
    {
        @Override
        public int getPartition(Text key,Text val,int numReduceTasks)
        {
            String[] str= val.toString().split(",");
            String year=str[1];
            if(year.equals("2011"))
            {
                return 0;
            }
            else
                if(year.equals("2012"))
                {
                    return 1;
                }
                else
                    if(year.equals("2013"))
                    {
                        return 2;
                    }
                    else
                        if(year.equals("2014"))
                        {
                            return 3;
                        }
                        else
                            if(year.equals("2015"))
                            {
                                return 4;
                            }
                            else
                            {
                                return 5;
                            }
        }
    }
   
    public static class reduceclass extends Reducer<Text,Text,NullWritable,Text>
    {
        TreeMap<IntWritable,Text> map=new TreeMap<IntWritable,Text>();
        String year="";
        public void reduce(Text key,Iterable<Text> val,Context context) throws IOException, InterruptedException
        {
            int count=0;
            for(Text str:val)
            {
                String[] ar = str.toString().split(",");
                count+=Integer.parseInt(ar[0]);
                year=ar[1];
            }
            String value1=year+","+key+","+count;
        //    String val1=count+","+year;
                //context.write(key,new Text(count+","+year));   
                map.put(new IntWritable(count),new Text(value1));
                if(map.size()>5)
                {
                    map.remove(map.firstKey());
                }
               
               
           
        }
           
        
            
         protected void cleanup(Context context) throws IOException,InterruptedException
           {
             
             for(Text value2:map.descendingMap().values())
                {
                    context.write(NullWritable.get(),new Text(value2));
                }
           }
        }
       


   
    public static void main(String[] args) throws Exception
    {
        Configuration conf=new Configuration();
        Job job = Job.getInstance(conf, "H1b_Visa 5th question");
        job.setJarByClass(proj5a.class);
        job.setMapperClass(mapclass.class);
        job.setReducerClass(reduceclass.class);
        job.setNumReduceTasks(6);
       job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
       //job.setCombinerClass(reduceclass.class);
       job.setPartitionerClass(part.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
