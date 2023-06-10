package com.MIE1628;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;


public class KMeans {

  public static int K;
  private static Utils utils;
  private static Configuration conf;
  

  public static class Point{

    public double x;
    public double y;

    public Point(double x, double y){
      this.x = x;
      this.y = y;
    }


    @Override
    public String toString() {
        return x + "," + y;
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

    public void add(Point b){
      this.x += b.x;
      this.y += b.y;
    }

    public void div(int b){
      this.x /= b;
      this.y /= b;
    }

    public Point(String s){
      String[] parts = s.split(",");
      this.x = Double.parseDouble(parts[0].replaceAll("\\s", ""));
      this.y = Double.parseDouble(parts[1].replaceAll("\\s", ""));
    }
  }

  
  public static class Map extends Mapper<LongWritable, Text, IntWritable, Text>{
    
    public static List<Point> centroids = new ArrayList<Point>();

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      
      conf = context.getConfiguration();
      String outDir = (new Path(conf.get("outputDir"))).getParent().toString();
      utils = new Utils(context.getConfiguration(), outDir, true);
      K = conf.getInt("K", 3);

      utils.log(context.getJobName(), "Getting centroids...");


      Integer iteration = conf.getInt("iteration", 0);
      
      if (iteration > 0) {
          String prevOutputDir = conf.get("outputDir") + "/iteration" + (iteration - 1);
          FileSystem fs = FileSystem.get(conf);
          FileStatus[] files = fs.listStatus(new Path(prevOutputDir));
          for (FileStatus file : files) {
              if (!file.isDirectory() && !file.getPath().getName().startsWith("_")) {
                  FSDataInputStream inputStream = fs.open(file.getPath());
                  BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                  String line;
                  int count = 0;
                  centroids = new ArrayList<Point>();
                  while ((line = reader.readLine()) != null && count < K) {
                    Scanner scanner = new Scanner(line);
                    scanner.next(); // Do not need the index 
                    centroids.add(new Point(scanner.next()));
                    count++;
                    scanner.close();
                  }
                  reader.close();
                  break;
              }
          }
      }
      else {
        centroids.add(new Point(-9.293805975483108,8.886169685374657));
        centroids.add(new Point(34.62862904699268,5.535490961190803));
        centroids.add(new Point(39.51585386054981, -1.3613455916076407));

        if (K == 6){
          centroids.add(new Point(33.920720753353066,7.415095526403888));
          centroids.add(new Point(35.961087794335654,0.950244176833094));
          centroids.add(new Point(7.261294784504573,17.495523876670845));
        }
      }
      String msg = "Centroids: [";
      for (Point centroid : centroids){
        msg += centroid.toString() + "\t";
      }
      msg += "]\n";
      utils.log(context.getJobName(), msg);
    }
    

    @Override
    public void map(LongWritable key, Text value, Context context) 
    throws IOException, InterruptedException{
      
      if (!value.toString().isEmpty()){
        String[] stringPoints = Utils.toStringPoints(value.toString());
        if (stringPoints.length == 2 && 
          !(stringPoints[0].isEmpty() || stringPoints[1].isEmpty())){

          Point point = new Point(value.toString());
    
          double minDistance = Double.MAX_VALUE;
          Integer closestIndex = -1;
          for (int i = 0; i < K; i++) {
              Point centroid = centroids.get(i);
              double distance = point.dist(centroid);
              if (distance < minDistance) {
                  minDistance = distance;
                  closestIndex = i;
              }
          }
          context.write(new IntWritable(closestIndex), new Text(point.toString()));
        }

      }

    } 
    @Override 
    public void cleanup(Context context) throws IOException, InterruptedException {
      utils.close();
    }

  }

  public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text>{
    public void reduce(IntWritable key, Iterable<Text> values, Context context) 
    throws IOException, InterruptedException {

      Point sum = new Point(0, 0);
      int count = 0;
      for (Text pointText : values) {
          sum.add(new Point(pointText.toString()));
          count++;
      }
      sum.div(count);
      context.write(key, new Text(sum.toString()));

    }
  }
  
}
