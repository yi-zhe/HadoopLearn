package com.hadoop.learn.com.hadoop.learn.ch5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.net.URI;

/**
 * @author Ly on 2018/6/29.
 *         写SequenceFile
 */
public class E5_10 {

    public static final String[] DATA = {
            "One, two, buckle my shoe",
            "Three, four, shut the door",
            "Five, six, pick up sticks",
            "Seven, eight, lay them straight",
            "Nine, ten, a big fat hen"
    };

    public static void main(String[] args) throws Exception {
        String uri = "hdfs://192.168.0.210/test/e5_10.out";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(uri);

        IntWritable key = new IntWritable();
        Text value = new Text();
        SequenceFile.Writer writer = null;
        try {
            writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
            for (int i = 0; i < 100; i++) {
                key.set(100 - i);
                value.set(DATA[i % DATA.length]);
                System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
                writer.append(key, value);
            }
        } finally {
            IOUtils.closeStream(writer);
        }
    }
}
