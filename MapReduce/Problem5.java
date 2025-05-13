package MapReduce;
/*
 * Problem5.java
 * 
 * CS 460: Problem Set 5
 * 
 * Chandini Toleti - U29391556
 * 
 */

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Problem5 {
    /*** mapper and reducer for the first job in the chain */
    public static class MyMapper1
      extends Mapper<Object, Text, Text, LongWritable> 
    { public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException 
      {
        String line = value.toString();
        String field = line.split(";")[0];
        String [] fields = field.split(",");
        if (fields.length<5){
          System.err.println("skipping bad input: " + line);
          return;
        } else if(fields.length==5 && fields[4].contains("@")){
          System.err.println("skipping bad input: " + line);
          return;
        }
        for(int i=4;i<fields.length; i++){
          if(fields[i].contains("@")){
            continue; 
          }
          context.write(new Text(fields[i]), new LongWritable(1));          
        }

      }


    }

    public static class MyReducer1
      extends Reducer<Text, LongWritable, Text, LongWritable> 
    {
      public void reduce(Text key, Iterable<LongWritable> values,
			                     Context context)
        throws IOException, InterruptedException 
        {
          long count=0; 
          for(LongWritable val:values){
            count=count+val.get(); 
          }
          context.write(key, new LongWritable(count)); 

        }

    }

    /*** mapper and reducer for the second job in the chain */
    public static class MyMapper2
      extends Mapper<Object, Text, Text, Text> 
    {
      public void map(Object key, Text value, Context context)
          throws IOException, InterruptedException 
        {
            String key_constant= "group sum"; 
            String [] fields= value.toString().split("\t"); 
            if(fields.length==2){
                context.write(new Text(key_constant), new Text(fields[0] + "," + fields[1]));
            }
            else{
                System.err.println("error in mapper input: " + value.toString());
            }
    
        }
      


    }

    public static class MyReducer2
      extends Reducer<Text, Text, Text, LongWritable> 
    {
      public void reduce(Text key, Iterable<Text> values,
			                     Context context)
          throws IOException, InterruptedException 
        {
            long max_count = 0;
            String max_group = null;
            System.out.println("reducer2 input key " + key.toString()); 
            for (Text val : values) {
                System.out.println("reducer2 input value " + val.toString()); 
                String[] fields = val.toString().split(",");
                if(fields.length<2){
                    System.err.println("skipping bad input: " + val.toString());
                    continue; 
                }
            
                String group = fields[0];
                long count = Long.parseLong(fields[1]);
                if (count > max_count) {
                  max_count = count;
                  max_group = group; 
                }
            }
 
            if (max_group != null) {
                context.write(new Text(max_group), new LongWritable(max_count));
            }
        }


    }

    public static void main(String[] args) throws Exception {
        /*
         * First job in the chain of two jobs
         */
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "problem 5-1");
        job1.setJarByClass(Problem5.class);

        // Specifies the names of the first job's mapper and reducer classes.
        job1.setMapperClass(MyMapper1.class);
        job1.setReducerClass(MyReducer1.class);

        // Sets the types for the keys and values output by the first mapper.
        /* CHANGE THE CLASS NAMES AS NEEDED IN THESE TWO METHOD CALLS */
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(LongWritable.class);

        // Sets the types for the keys and values output by the first reducer.
        /* CHANGE THE CLASS NAMES AS NEEDED IN THESE TWO METHOD CALLS */
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);

        // Configure the type and location of the data processed by job1.
        job1.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));

        // Specify where job1's results should be stored.
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);

        /*
         * Second job the chain of two jobs
         */
        conf = new Configuration();
        Job job2 = Job.getInstance(conf, "problem 5-2");
        job2.setJarByClass(Problem5.class);

        // Specifies the names of the second job's mapper and reducer classes.
        job2.setMapperClass(MyMapper2.class);
        job2.setReducerClass(MyReducer2.class);

        // Sets the types for the keys and values output by the second mapper.
        /* CHANGE THE CLASS NAMES AS NEEDED IN THESE TWO METHOD CALLS */
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        
        // Sets the types for the keys and values output by the second reducer.
        /* CHANGE THE CLASS NAMES AS NEEDED IN THESE TWO METHOD CALLS */
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);

        // Configure the type and location of the data processed by job2.
        // Note that its input path is the output path of job1!
        job2.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        job2.waitForCompletion(true);
    }
}
