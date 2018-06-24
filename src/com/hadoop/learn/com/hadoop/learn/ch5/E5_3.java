package com.hadoop.learn.com.hadoop.learn.ch5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * @author Ly on 2018/6/25.
 *         使用CodecPool的例子
 */
public class E5_3 {

    public static void main(String[] args) throws Exception {
        String codecClassname = "org.apache.hadoop.io.compress.GzipCodec";
        Class<?> codecClass = Class.forName(codecClassname);
        Configuration conf = new Configuration();
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
        Compressor compressor = null;
        try {
            compressor = CodecPool.getCompressor(codec);
            CompressionOutputStream out = codec.createOutputStream(new FileOutputStream("/soft/code/output/e5_3.out"), compressor);
            // e5_3.in是一个未压缩的文本, 输出文件是个gzip压缩的文本
            IOUtils.copyBytes(new FileInputStream("/soft/code/input/e5_3.in"), out, 4096, true);
        } finally {
            CodecPool.returnCompressor(compressor);
        }
    }
}
