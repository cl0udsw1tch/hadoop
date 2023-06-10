package com.MIE1628;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class App {

    private static Utils utils;
    private final static int MAX_ITERS = 5;
    public static void main( String[] args ) throws Exception {

      final String SHAKESPEARE_PATH = args[0];
      final String DATAPOINTS_PATH = args[1];
      final String OUTPUT_DIR = args[2];
      
      
      Configuration wordCountConf = new Configuration();
      
      if (args.length == 4){
        if (args[3].equals("--overwrite") || args[3].equals("-o")){
          Path path = new Path(OUTPUT_DIR);
          FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:9000"), wordCountConf);
          if (hdfs.exists(path)) {
            hdfs.delete(path, true);
            hdfs.mkdirs(path);
            
            Utils.print("Refreshed previous hdfs output directory.");
          }
        }
      }
      
      utils = new Utils(wordCountConf, OUTPUT_DIR, false);
      Utils.print("Logging to " + utils.logFilePath.toString());
      Utils.print(
        "shakespeare.txt path: " + SHAKESPEARE_PATH + 
        "\ndata_points.txt path: " + DATAPOINTS_PATH + 
        "\noutput directory: " + OUTPUT_DIR
      );
      Utils.print("Starting Job 1: Word Count...");
      Job jobWordCount = Job.getInstance(wordCountConf, "WordCount");

  
      jobWordCount.setJarByClass(WordCount.class);
      jobWordCount.setMapperClass(WordCount.Map.class);
      jobWordCount.setReducerClass(WordCount.Reduce.class);
      //jobWordCount.setCombinerClass(Reduce.class);
      jobWordCount.setOutputKeyClass(Text.class);
      jobWordCount.setOutputValueClass(IntWritable.class);
      jobWordCount.setInputFormatClass(TextInputFormat.class);
      jobWordCount.setOutputFormatClass(TextOutputFormat.class);
      FileInputFormat.setInputPaths(jobWordCount, new Path(SHAKESPEARE_PATH));
      FileOutputFormat.setOutputPath(jobWordCount, new Path(OUTPUT_DIR + "/WordCount"));

      if (!jobWordCount.waitForCompletion(true)){
        System.exit(1);
      }
      Utils.print("Finished Word Count.");



      final String K3MEANSDIR = OUTPUT_DIR + "/K3Means";
      final String K6MEANSDIR = OUTPUT_DIR + "/K6Means";

      Utils.print("Starting KMeans jobs (k=3)...");
      Configuration k3MeansConf = new Configuration();
      k3MeansConf.setInt("K", 3);

      boolean converged = false;
      Integer iter = 0;
      while (!converged && iter < MAX_ITERS){

        Utils.print("Iteration: " + iter.toString());

        k3MeansConf.setInt("iteration", iter);
        k3MeansConf.set("outputDir", K3MEANSDIR);

        Job jobK3Means = Job.getInstance(k3MeansConf, "K3Means:" + "iteration=" + iter);
        jobK3Means.setJarByClass(KMeans.class);
        jobK3Means.setMapperClass(KMeans.Map.class);
        jobK3Means.setReducerClass(KMeans.Reduce.class);
        //jobK3Means.setCombinerClass(Reduce.class);
        jobK3Means.setOutputKeyClass(IntWritable.class);
        jobK3Means.setOutputValueClass(Text.class);

        jobK3Means.setInputFormatClass(TextInputFormat.class);
        jobK3Means.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(jobK3Means, new Path(DATAPOINTS_PATH));
        FileOutputFormat.setOutputPath(jobK3Means, new Path(K3MEANSDIR + "/iteration" + iter));

        if (!jobK3Means.waitForCompletion(true)){
          System.exit(1);
        }
        iter++;
      }
      Utils.print("Finished KMeans (k=3).");




      Utils.print("Starting KMeans jobs (k=6)...");
      Configuration k6MeansConf = new Configuration();
      k6MeansConf.setInt("K", 6);

      boolean _converged = false;
      int _iter = 0;
      while (!_converged && _iter < MAX_ITERS){

        k6MeansConf.setInt("iteration", _iter);
        k6MeansConf.set("outputDir", K6MEANSDIR);

        Utils.print("Iteration: " + iter.toString());

        Job jobK6Means = Job.getInstance(k6MeansConf, "K6Means:" + "iteration=" + _iter);
        jobK6Means.setJarByClass(KMeans.class);
        jobK6Means.setMapperClass(KMeans.Map.class);
        jobK6Means.setReducerClass(KMeans.Reduce.class);
        //jobK6Means.setCombinerClass(Reduce.class);
        jobK6Means.setOutputKeyClass(IntWritable.class);
        jobK6Means.setOutputValueClass(Text.class);

        jobK6Means.setInputFormatClass(TextInputFormat.class);
        jobK6Means.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(jobK6Means, new Path(DATAPOINTS_PATH));
        FileOutputFormat.setOutputPath(jobK6Means, new Path(K6MEANSDIR + "/iteration" + _iter));

        if (!jobK6Means.waitForCompletion(true)) {
          System.exit(1);
        }
        _iter++;

      }
      Utils.print("Finished KMeans (k=6).");
      utils.copyToLocal();
      System.exit(0);
    }
  
  
}

