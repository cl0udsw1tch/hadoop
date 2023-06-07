package com.MIE1628;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;


public class KMeans {

  private class Point{
    public final double x;
    public final double y;
    public Point(double x, double y){
      this.x = x;
      this.y = y;
    }

    public Point nearest(Point[] centroids){
      Point closestPoint = centroids[0];
      double minDistance = this.dist(closestPoint);
      for (Point centroid : centroids){
        if (this.dist(centroid) < minDistance){
          minDistance = this.dist(centroid);
          closestPoint = centroid;
        }
      }
      return closestPoint;
    }

    public double dist(Point b){
      return Math.sqrt(Math.pow(this.x - b.x, 2) + Math.pow(this.y - b.y, 2));
    }
  }

  
  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
    
    public static Point[] centroids;

    public void setup(Context context) throws IOException, InterruptedException {
      
    }
    
    public void map(LongWritable key, Text value, Context context) 
    throws IOException, InterruptedException{

    }

    public void cleanup(Context context) throws IOException, InterruptedException {

    }

  }

  public static class Reduce extends Reducer<Text, IntWritable, Text, LongWritable>{
    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
    throws IOException, InterruptedException {

    }
  }
  
}
