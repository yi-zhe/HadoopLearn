package com.hadoop.learn.com.hadoop.learn.ch3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * @author Ly on 2018/6/23.
 *         使用FileSystem API写文件
 */
public class E3_4 {
    public static void main(String[] args) throws Exception {
        InputStream in = new BufferedInputStream(new FileInputStream("D:/a.txt"));
        // 此目录创建时指定了任意用户可写
        String dst = "hdfs://192.168.0.210/write/a.txt";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        OutputStream out = fs.create(new Path(dst), new Progressable() {
            @Override
            public void progress() {
                System.out.print(".");
            }
        });
        // 写文件时如果文件不是utf-8编码, 则会有乱码
        IOUtils.copyBytes(in, out, 4096, true);
    }
}
