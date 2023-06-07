package com.MIE1628;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class App 
{

    public static void main( String[] args ) throws Exception {
      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "KMeans");
      job.setJarByClass(App.class);
      job.setMapperClass(WordCount.Map.class);
      job.setReducerClass(WordCount.Reduce.class);
      //job.setCombinerClass(Reduce.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      FileInputFormat.setInputPaths(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));


      
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
  
  
}

