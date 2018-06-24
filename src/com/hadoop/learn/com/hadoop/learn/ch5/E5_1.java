package com.hadoop.learn.com.hadoop.learn.ch5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;

/**
 * @author Ly on 2018/6/24.
 *         数据压缩与解压缩
 *         将a.txt的内容使用gzip压缩后写入a.gz中
 */
public class E5_1 {
    public static void main(String[] args) throws Exception {
        String codecName = "org.apache.hadoop.io.compress.GzipCodec";
        Class<?> codecClass = Class.forName(codecName);
        Configuration conf = new Configuration();
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
        FileOutputStream outputStream = new FileOutputStream("/soft/a.gz");
        CompressionOutputStream out = codec.createOutputStream(outputStream);
        InputStream in = new FileInputStream("/soft/a.txt");
        IOUtils.copyBytes(in, out, 4096, false);
        out.finish();
    }
}
