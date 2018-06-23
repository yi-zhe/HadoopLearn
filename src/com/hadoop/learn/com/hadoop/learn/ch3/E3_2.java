package com.hadoop.learn.com.hadoop.learn.ch3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;

/**
 * @author Ly on 2018/6/23.
 *         FileSystem read data
 */
public class E3_2 {

    /**
     * @see org.apache.hadoop.fs.FileSystem#getLocal(Configuration)
     * @see org.apache.hadoop.fs.FileSystem#get(Configuration)
     * @see org.apache.hadoop.fs.FileSystem#get(URI, Configuration)
     * @see org.apache.hadoop.fs.FileSystem#get(URI, Configuration, String)
     */
    public static void main(String[] args) throws IOException {
        String uri = "hdfs://192.168.0.210/word.in";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        InputStream in = null;
        try {
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
