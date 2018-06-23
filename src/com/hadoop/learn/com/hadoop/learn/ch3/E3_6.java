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
public class E3_6 {
    public static void main(String[] args) throws Exception {
        String dst = "hdfs://192.168.0.210/write";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        FileStatus[] stats = fs.listStatus(new Path(dst));
        for (FileStatus stat : stats)
            System.out.println(stat.toString());
    }
}
