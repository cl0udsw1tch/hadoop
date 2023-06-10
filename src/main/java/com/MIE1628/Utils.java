package com.MIE1628;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.*;
import java.time.format.DateTimeFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class Utils {

  public String outDir;
  private Configuration conf;
  private FileSystem hdfs;
  public Path logFilePath;
  private BufferedWriter logFileStream;

  public Utils(Configuration conf, String outputDir, boolean inMapRed) throws IOException {

    this.outDir = outputDir;
    this.conf = conf;
    this.hdfs = FileSystem.get(conf);
    if (inMapRed){
      FileStatus file = this.hdfs.listStatus(new Path(this.outDir +  "/logs"))[0];
      this.logFilePath = file.getPath();
      this.logFileStream = new BufferedWriter(
        new OutputStreamWriter(this.hdfs.append(this.logFilePath), 
        StandardCharsets.UTF_8));
      
    }
    else {
      this.hdfs.mkdirs(new Path(outputDir + "/logs"));
      this.logFilePath = new Path(outputDir + "/logs/" + Utils.formatDateTime() + ".log");
      FSDataOutputStream out = this.hdfs.create(this.logFilePath);
      out.close();
      File localLogDir = new File("logs");
      if (!localLogDir.exists()){
        localLogDir.mkdirs();
      }

      File localOutput = new File("output");
      if (localOutput.exists()){
        Utils.deleteLocalDir("output");
      }
    }
  }

  public static void print(String msg){
    System.out.println("[A1]: " + msg);
  }

  public void log(String job, String msg) throws IOException{
    this.logFileStream.write("[" + job + "]-[" + LocalTime.now() + "]: " + msg + "\n");
  }
  public void log(String msg) throws IOException{
    this.logFileStream.write(msg + "\n");
  }

  public static String formatDateTime(){
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
    String fileName = LocalDateTime.now().format(formatter);
    return fileName;
  }

  public static String[] toStringPoints(String s){
    String _s = s.replaceAll("\\s", "");
    return _s.split(",");
  }


  public void close() throws IOException{
    this.logFileStream.close();
  }

  public void copyToLocal() throws IOException{

    Path hdfsPath = new Path(this.outDir);
    Path localPath = new Path(".");
    this.hdfs.copyToLocalFile(false, hdfsPath, localPath, false);
    Utils.print("Copied output directory [hdfs] into source directory [local].");

    java.nio.file.Path source = Paths.get("output/logs/" + this.logFilePath.getName());
    java.nio.file.Path destination = Paths.get("logs/" + this.logFilePath.getName());
    Files.copy(source, destination);
    Utils.deleteLocalDir("output/logs");
  }

  public static void deleteLocalDir(String directory){
    File file = new File(directory);
    for (File subFile : file.listFiles()) {
      if(subFile.isDirectory()) {
         Utils.deleteLocalDir(subFile.getAbsolutePath());
      } else {
         subFile.delete();
      }
   }
   file.delete();
  }
}
