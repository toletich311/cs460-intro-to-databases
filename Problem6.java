/*
 * Problem6.java
 * 
 * CS 460: Problem Set 4
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

public class Problem6 {
    public static class MyMapper
      extends Mapper<Object, Text, Text, Text> 
    {
      public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException 
      {
        String line = value.toString();
        String field = line.split(";")[0];
        String [] fields = field.split(",");

        int count=0; 
        String key_constant= "id group_num"; 

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
          count=count+1;          
        }

        String value_field = fields[0]+","+String.valueOf(count); 
        context.write(new Text(key_constant), new Text(value_field)); 

      }

    }

    public static class MyReducer
      extends Reducer<Text, Text, Text, IntWritable> 
    {
      public void reduce(Text key, Iterable<Text> values,
			                     Context context)
          throws IOException, InterruptedException 
        {
            int max_count = 0;
            String max_id = null;
            System.out.println("reducer2 input key " + key.toString()); 
            for (Text val : values) {
                System.out.println("reducer2 input value " + val.toString()); 
                String[] fields = val.toString().split(",");
                if(fields.length<2){
                    System.err.println("skipping bad input: " + val.toString());
                    continue; 
                }
            
                String id = fields[0];
                int count = Integer.parseInt(fields[1]);
                if (count > max_count) {
                  max_count = count;
                  max_id = id; 
                }
            }
 
            if (max_id != null) {
                context.write(new Text(max_id), new IntWritable(max_count));
            }
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "problem 6");
        job.setJarByClass(Problem6.class);

        // Specifies the names of the mapper and reducer classes.
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // Sets the types for the keys and values output by the mapper.
        /* CHANGE THE CLASS NAMES AS NEEDED IN THESE TWO METHOD CALLS */
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Sets the types for the keys and values output by the reducer.
        /* CHANGE THE CLASS NAMES AS NEEDED IN THESE TWO METHOD CALLS */
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Configure the type and location of the data being processed.
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // Specify where the results should be stored.
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
