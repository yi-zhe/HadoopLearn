package com.hadoop.learn.com.hadoop.learn.ch3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * @author Ly on 2018/6/23.
 *         FileStatus
 */
public class E3_5 {
    public static void main(String[] args) throws Exception {
        String dst = "hdfs://192.168.0.210/write/a.txt";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        FileStatus stat = fs.getFileStatus(new Path(dst));
        System.out.println(stat.toString());
    }
}
