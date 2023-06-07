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

public class WordCount {
  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key, Text value, Context context) 
    throws IOException, InterruptedException {

      StringTokenizer tokenizer = new StringTokenizer(value.toString());
      while (tokenizer.hasMoreTokens()) {

        String currToken = tokenizer
        .nextToken()
        .replaceAll("^[^a-zA-Z]+|[^a-zA-Z]+$", "");

        value.set(currToken);
        context.write(value, new IntWritable(1));
      }
    }

  }

  public static class Reduce extends Reducer<Text, IntWritable, Text, LongWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
    throws IOException, InterruptedException {

      long sum = 0;
      Iterator<IntWritable> iter = values.iterator();
      while(iter.hasNext()){
        sum++;
        iter.next();
      }
      LongWritable out = new LongWritable(sum);
      context.write(key, out);
    }
  }
}
