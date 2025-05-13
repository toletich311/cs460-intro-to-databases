/*
 * Problem3.java
 * 
 * CS 460: Problem Set 4
 * 
 * Chandini Toleti and Javeria Jalil
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

public class Problem3 {
    public static class MyMapper
      extends Mapper<Object, Text, Text, IntWritable> 
    {
        public void map(Object key, Text value, Context context)
          throws IOException, InterruptedException 
        {
            /* Define your map method here. */
            String line= value.toString(); 
            if(line.contains("@")){
              String [] fields= line.split("@"); 
            
              if(fields.length==2){
                String email_name=fields[1].split("[,;]")[0];
                context.write(new Text(email_name), new IntWritable(1)); 
              }
            }


        }
    }

    public static class MyReducer
      extends Reducer<Text, IntWritable, Text, LongWritable> 
    {
        public void reduce(Text key, Iterable<IntWritable> values,
			                     Context context)
          throws IOException, InterruptedException 
        {
            /* Define your reduce method here. */
            long total=0; 
            for(IntWritable val: values){
                 total+=val.get(); 
            }
            context.write(key, new LongWritable(total)); 



        }
    }

    public static void main(String[] args) throws Exception {
        /* 
         * Configures and runs the MapReduce job for this problem,
         * using the Mapper and Reducer classes that you will 
         * define above.
         * 
         * IMPORTANT: You should NOT actually try to run the program locally.
         * Rather, after eliminating syntax errors, you should 
         * run your program using the Gradescope page we have
         * provided. See the problem set for more details.
         * 
         * You should NOT change this method!
         */

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "problem 3");

        // Specifies the name of the outer class.
        job.setJarByClass(Problem3.class);

        // Specifies the names of the mapper and reducer classes.
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // Sets the types for the keys and values output by the mapper.
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Sets the types for the keys and values output by the reducer.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // Configure the type and location of the data being processed.
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // Specify where the results should be stored.
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
