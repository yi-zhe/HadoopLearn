package com.hadoop.learn.com.hadoop.learn.ch3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.net.URI;

/**
 * @author Ly on 2018/6/23.
 *         PathFilter
 */
public class E3_7 {
    public static void main(String[] args) throws Exception {
        String dst = "hdfs://192.168.0.210/write/*";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        FileStatus[] stats = fs.globStatus(new Path(dst), new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().matches("^a\\.\\w+");
            }
        });
        for (FileStatus stat : stats)
            System.out.println(stat.toString());
    }
}
